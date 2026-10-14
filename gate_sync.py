#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
VPN Gate 节点检测与数据同步脚本
=============================================================================
功能概述:
- 联动 Cloudflare Workers 边缘网关，定时获取最新待测节点与复测历史节点
- 与 Web 控制台深度对齐：支持 TCP 快速模式与 SSTP 真连接模式、历史差量复测与状态回写
- 内置 HTTP 连接复用、网络层指数退避、401 凭据过期自动重登与并发容灾隔离
=============================================================================
"""

from __future__ import annotations

import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# 统一控制台输出编码，防止 Windows / Linux 控制台因特殊符号异常
for _s in (sys.stdout, sys.stderr):
    try:
        _s.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

# =============================================================================
# 1. 配置参数读取 (环境变量驱动)
# =============================================================================
BASE_URL = os.environ.get("GATE_URL", "").rstrip("/")
PASSWORD = os.environ.get("GATE_PASSWORD", "")
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "30"))
CONCURRENCY = max(1, int(os.environ.get("CONCURRENCY", "2")))
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "3"))
REQUEST_TIMEOUT = int(os.environ.get("REQUEST_TIMEOUT", "30"))
TIME_SLEEP = int(os.environ.get("TIME_SLEEP", "3"))

CHECK_MODE = "tcp" if os.environ.get("CHECK_MODE", "").strip().lower() == "tcp" else "sstp"
FETCH_COUNTRY = os.environ.get("FETCH_COUNTRY", "ALL").strip().upper()
DIFF_MODE = os.environ.get("DIFF_MODE", "true").strip().lower() in ("true", "1", "yes")
CLEAN_DEAD = os.environ.get("CLEAN_DEAD", "false").strip().lower() in ("true", "1", "yes")


# =============================================================================
# 2. 网络会话与底层通信层
# =============================================================================
def create_http_session(concurrency: int) -> requests.Session:
    """创建带有连接复用与网络层自动重试的 HTTP 会话"""
    sess = requests.Session()
    pool_size = max(10, concurrency * 2)
    retry_strategy = Retry(
        total=2,
        backoff_factor=0.5,
        status_forcelist=[502, 503, 504],
        raise_on_status=False
    )
    adapter = HTTPAdapter(
        pool_connections=pool_size,
        pool_maxsize=pool_size,
        max_retries=retry_strategy
    )
    sess.mount("http://", adapter)
    sess.mount("https://", adapter)
    sess.headers.update({
        "User-Agent": "Gate-Sync/2.1 (Automated Worker Syncer)",
        "Content-Type": "application/json"
    })
    return sess


session = create_http_session(CONCURRENCY)


def login() -> bool:
    """向 Worker 发送密码建立管理员认证会话"""
    url = f"{BASE_URL}/admin/login"
    try:
        resp = session.post(url, json={"password": PASSWORD}, timeout=min(15, REQUEST_TIMEOUT))
        return resp.ok and resp.json().get("success", False)
    except Exception as exc:
        print(f"      [认证异常] 连接失败: {exc}")
        return False


def post_api(path: str, data: Optional[Dict[str, Any]] = None, allow_reauth: bool = True) -> Dict[str, Any]:
    """
    向 Worker API 发送 POST 请求
    - 支持失败自动重试与超时防护
    - 支持 401 凭据过期透明自动重新登录
    """
    url = f"{BASE_URL}{path}"
    last_error = ""

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.post(url, json=data or {}, timeout=REQUEST_TIMEOUT)
            if resp.ok:
                return resp.json()

            # 401 凭据过期自动自愈重登
            if resp.status_code == 401 and allow_reauth:
                print(f"      [凭据告警] {path} 返回 401 未授权，尝试重新登录自愈...")
                if login():
                    return post_api(path, data, allow_reauth=False)

            last_error = f"HTTP {resp.status_code} ({resp.text[:100]})"
            print(f"      [重试警告] {path} 返回 {last_error} ({attempt}/{MAX_RETRIES})")
        except Exception as exc:
            last_error = str(exc)
            print(f"      [重试警告] {path} 网络请求异常: {last_error} ({attempt}/{MAX_RETRIES})")

        if attempt < MAX_RETRIES:
            time.sleep(TIME_SLEEP)

    raise RuntimeError(f"POST {path} 连续失败 {MAX_RETRIES} 次，最终错误: {last_error}")


# =============================================================================
# 3. 业务处理与并发调度层
# =============================================================================
def filter_by_country(nodes: List[Dict[str, Any]], country: str) -> List[Dict[str, Any]]:
    """按国家代码过滤节点 (ALL 或为空则不过滤)"""
    target = country.strip().upper()
    if not target or target == "ALL":
        return nodes
    return [n for n in nodes if (n.get("country_code") or "??").upper() == target]


def check_nodes(
    nodes: List[Dict[str, Any]],
    batch_size: int = BATCH_SIZE,
    concurrency: int = CONCURRENCY,
    mode: str = CHECK_MODE
) -> List[Dict[str, Any]]:
    """
    并发分批向 Worker 提交节点连通性检测任务
    - 采用 ThreadPoolExecutor 进行多通道并发调度
    - 具备单批次故障降级隔离，某批次超时不会导致其他批次成果丢失
    """
    total = len(nodes)
    if total == 0:
        return []

    chunks = [nodes[i:i + batch_size] for i in range(0, total, batch_size)]
    mode_label = "真连接 (SSTP)" if mode == "sstp" else "快速 (TCP)"
    workers = min(concurrency, len(chunks))
    completed = 0
    all_results: List[Dict[str, Any]] = []

    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_to_chunk = {
            executor.submit(post_api, "/admin/api/check-nodes", {"nodes": c, "mode": mode}): c
            for c in chunks
        }
        for future in as_completed(future_to_chunk):
            chunk = future_to_chunk[future]
            chunk_len = len(chunk)
            completed += chunk_len
            try:
                res = future.result()
                if isinstance(res, list):
                    all_results.extend(res)
            except Exception as exc:
                print(f"  ❌ 批次检测失败 ({chunk_len} 节点): {exc}")
                # 针对异常批次安全标记为失效，确保后续状态依然可以完整回写
                for n in chunk:
                    all_results.append({
                        **n,
                        "alive": False,
                        "latency": -1,
                        "error": str(exc),
                        "check_mode": mode
                    })
            print(f"  -> 检测进度 [{mode_label}]: {completed} / {total} 个节点...")

    return all_results


# =============================================================================
# 4. 主同步执行流程
# =============================================================================
def main() -> None:
    """主同步执行管线：登录 -> 获取待测节点 -> 并发检测 -> 差量复测 -> 回写入库 -> 可选清理"""
    if not BASE_URL:
        print("❌ 错误: 未配置 GATE_URL 环境变量！", file=sys.stderr)
        sys.exit(1)

    print(f"[1/5] 正在登录 CF Workers Gate 站点: {BASE_URL} ...")
    if not login():
        print("❌ 登录失败: 密码错误或站点无响应！", file=sys.stderr)
        sys.exit(1)
    print("      ✓ 登录成功并已建立会话！")

    mode_label = "真连接模式 (SSTP)" if CHECK_MODE == "sstp" else "快速模式 (TCP)"
    scope_label = "全部地区" if FETCH_COUNTRY == "ALL" else f"指定地区 [{FETCH_COUNTRY}]"

    # [2/5] 获取待测最新节点
    print(f"[2/5] 获取 VPN Gate 最新待测节点 ({scope_label})...")
    raw_new_resp = post_api("/admin/api/get-new-nodes")
    raw_new = raw_new_resp.get("nodes", []) if isinstance(raw_new_resp, dict) else []
    if not raw_new:
        print("      ⚠️ 官方源返回空列表，安全终止以防误清空！", file=sys.stderr)
        sys.exit(1)

    new_nodes = filter_by_country(raw_new, FETCH_COUNTRY)
    print(f"      ✓ 待测最新节点: {len(new_nodes)} 个 (模式: {mode_label}, 并发通道: {CONCURRENCY})")

    # [3/5] 检测最新节点并入库
    new_results = check_nodes(new_nodes, BATCH_SIZE, CONCURRENCY, CHECK_MODE)
    alive_new = [n for n in new_results if n.get("alive") or n.get("latency", -1) > 0]
    print(f"[3/5] 最新节点检测完成：存活 {len(alive_new)} / {len(new_nodes)} 个")

    if alive_new:
        payload: Dict[str, Any] = {"nodes": alive_new, "fetched": len(new_nodes)}
        if FETCH_COUNTRY != "ALL":
            payload["country"] = FETCH_COUNTRY
        post_api("/admin/api/save-new-nodes", payload)
        print("      ✓ 当前最新节点已更新入库！")
    elif new_nodes:
        print("      ⚠️ 检测存活数为 0 (可能由于网络波动)，触发熔断保底跳过更新！")

    # [4/5] 提取并检测历史节点 (支持差量复测)
    diff_desc = "开启差量剔除" if DIFF_MODE else "全量复测"
    print(f"[4/5] 提取待复测的历史节点 ({diff_desc})...")
    raw_hist_resp = post_api("/admin/api/get-history-nodes", {"diff": DIFF_MODE})
    raw_history = raw_hist_resp.get("nodes", []) if isinstance(raw_hist_resp, dict) else []
    history_nodes = filter_by_country(raw_history, FETCH_COUNTRY)
    print(f"      待复测历史节点: {len(history_nodes)} 个")

    if history_nodes:
        hist_results = check_nodes(history_nodes, BATCH_SIZE, CONCURRENCY, CHECK_MODE)
        alive_hist = [n for n in hist_results if n.get("alive") or n.get("latency", -1) > 0]
        dead_hist = [n for n in hist_results if not n.get("alive") or n.get("latency", -1) == -1]

        if not alive_hist and len(history_nodes) > 0:
            print("[5/5] ⚠️ 历史节点复测存活数为 0 (疑似网络异常)，触发熔断保底，跳过更新！")
        else:
            post_api("/admin/api/save-history-nodes", {"updated_nodes": hist_results})
            print(f"[5/5] 历史节点状态同步完成：可用 {len(alive_hist)} 个，失效 {len(dead_hist)} 个 (状态已回写，失效节点已保留)")
    else:
        print("[5/5] 暂无历史节点需要复测，跳过。")

    # [可选清理任务] 若配置 CLEAN_DEAD=true，则独立调用清理失效节点
    if CLEAN_DEAD:
        print("[额外任务] 正在执行清理失效节点...")
        clean_payload: Dict[str, Any] = {"country": FETCH_COUNTRY} if FETCH_COUNTRY != "ALL" else {}
        clean_resp = post_api("/admin/api/clean-dead-nodes", clean_payload)
        cleaned = clean_resp.get("removed", 0) if isinstance(clean_resp, dict) else 0
        print(f"      ✓ 清理失效节点完成：已移除 {cleaned} 个失效节点！")

    print(f"完成: 所有数据已成功同步至 Worker 节点网关 ({BASE_URL})")


if __name__ == "__main__":
    main()