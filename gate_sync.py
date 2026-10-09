#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
VPN Gate 节点检测与数据同步脚本
联动 Cloudflare Workers
"""

import os
import sys
import time
import requests

# ==============================================================================
# 全局配置参数 (支持通过 GitHub Actions 环境变量动态覆盖)
# ==============================================================================
BASE_URL = os.environ.get("GATE_URL", "").rstrip("/")
PASSWORD = os.environ.get("GATE_PASSWORD", "")
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "35"))
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "3"))
REQUEST_TIMEOUT = int(os.environ.get("REQUEST_TIMEOUT", "30"))
TIME_SLEEP = int(os.environ.get("TIME_SLEEP", "3"))

session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Gate-Sync/1.0",
    "Content-Type": "application/json"
})

def post_api(path, data=None):
    """统一向 Worker 发送 POST 请求（带自动重试与超时保护）"""
    url = f"{BASE_URL}{path}"
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.post(url, json=data or {}, timeout=REQUEST_TIMEOUT)
            if resp.ok:
                return resp.json()
            print(f"      [重试警告] {path} 返回 HTTP {resp.status_code} ({attempt}/{MAX_RETRIES})")
        except Exception as e:
            print(f"      [重试警告] {path} 网络请求异常: {e} ({attempt}/{MAX_RETRIES})")
        time.sleep(TIME_SLEEP)
    raise RuntimeError(f"POST {path} 连续失败 {MAX_RETRIES} 次")

def check_nodes(nodes, batch_size=BATCH_SIZE):
    """分批并发检测节点连通性"""
    results = []
    total = len(nodes)
    for i in range(0, total, batch_size):
        chunk = nodes[i:i + batch_size]
        print(f"  -> 检测进度: {i + 1} ~ {min(i + batch_size, total)} / {total} 个节点...")
        checked = post_api("/admin/api/check-nodes", {"nodes": chunk})
        results.extend(checked)
    return results

def main():
    print(f"[1/5] 正在登录 CF Workers Gate 站点 ...")
    login_resp = session.post(f"{BASE_URL}/admin/login", json={"password": PASSWORD}, timeout=15)
    if not login_resp.ok:
        print(f"❌ 登录失败: HTTP {login_resp.status_code}")
        sys.exit(1)
    print("      ✓ 登录成功！")

    print("[2/5] 获取 VPN Gate 官方最新节点列表...")
    new_data = post_api("/admin/api/get-new-nodes")
    new_nodes = new_data.get("nodes", [])
    if not new_nodes:
        print("      ⚠️ 官方源返回空列表，安全终止以防误清空！")
        sys.exit(1)
    print(f"      ✓ 获取到 {len(new_nodes)} 个节点，开始分批检测...")

    new_results = check_nodes(new_nodes, BATCH_SIZE)
    alive_new_nodes = [n for n in new_results if n.get("alive")]
    print(f"[3/5] 最新节点检测完成：存活 {len(alive_new_nodes)} / {len(new_nodes)} 个")

    if alive_new_nodes:
        post_api("/admin/api/save-new-nodes", {"nodes": alive_new_nodes, "fetched": len(new_nodes)})
        print("      ✓ 当前最新节点已更新！")
    else:
        # 存活为 0 时主动拦截并报警，保留上一轮正常数据
        print("      ⚠️ 检测存活节点为 0 (可能由于网络波动)，跳过更新以保护现有节点！")

    print("[4/5] 检查历史留存节点...")
    history_data = post_api("/admin/api/get-history-nodes")
    history_nodes = history_data.get("nodes", [])
    print(f"      待复测历史节点: {len(history_nodes)} 个")

    alive_history_nodes = []
    dead_history_keys = []

    if history_nodes:
        history_results = check_nodes(history_nodes, BATCH_SIZE)
        alive_history_nodes = [n for n in history_results if n.get("alive")]
        dead_history_keys = [f"{n['host'].lower()}:{n['port']}" for n in history_results if not n.get("alive")]
        if not alive_history_nodes and len(dead_history_keys) == len(history_nodes):
            print(f"[5/5] ⚠️ 历史节点复测存活数为 0 (可能由于网络波动)，跳过更新以保护历史节点！")
        else:
            print(f"[5/5] 更新历史节点: 保留 {len(alive_history_nodes)} 个，移除失效 {len(dead_history_keys)} 个")
            post_api("/admin/api/save-history-nodes", {
                "alive_nodes": alive_history_nodes,
                "dead_keys": dead_history_keys
            })
            print("      ✓ 历史节点已复测完成！")
    else:
        print("[5/5] 暂无历史节点需要复测，跳过。")

    print("完成: 数据已全部同步至CF Workers Gate")

if __name__ == "__main__":
    main()