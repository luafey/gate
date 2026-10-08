#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
VPN Gate 节点检测与数据同步脚本
联动 Cloudflare Workers
"""

import os
import sys
import requests

BASE_URL = os.environ.get("GATE_URL", "").rstrip("/")
PASSWORD = os.environ.get("GATE_PASSWORD", "")
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "35"))

session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Gate-Sync/1.0",
    "Content-Type": "application/json"
})

def post_json(path, data=None):
    url = f"{BASE_URL}{path}"
    resp = session.post(url, json=data or {}, timeout=60)
    if not resp.ok:
        raise RuntimeError(f"POST {path} 失败: HTTP {resp.status_code} - {resp.text}")
    return resp.json()

def get_json(path):
    url = f"{BASE_URL}{path}"
    resp = session.get(url, timeout=30)
    if not resp.ok:
        raise RuntimeError(f"GET {path} 失败: HTTP {resp.status_code} - {resp.text}")
    return resp.json()

def check_in_chunks(nodes, batch_size=20):
    results = []
    total = len(nodes)
    for i in range(0, total, batch_size):
        chunk = nodes[i:i + batch_size]
        print(f"  -> 检测进度: {i + 1} ~ {min(i + batch_size, total)} / {total} 个节点...")
        checked = post_json("/admin/api/check-chunk", {"chunk": chunk})
        results.extend(checked)
    return results

def main():
    print(f"[1/5] 正在登录 Worker 控制台: {BASE_URL} ...")
    login_resp = session.post(f"{BASE_URL}/admin/login", json={"password": PASSWORD}, timeout=15)
    if not login_resp.ok:
        print(f"❌ 登录失败: HTTP {login_resp.status_code}")
        sys.exit(1)
    print("      ✓ 登录成功！")

    print("[2/5] 获取 VPN Gate 官方最新节点列表...")
    official_data = get_json("/admin/api/prepare-official")
    candidates = official_data.get("candidates", [])
    print(f"      ✓ 获取到 {len(candidates)} 个候选节点，开始分批检测...")

    official_results = check_in_chunks(candidates, BATCH_SIZE)
    alive_official = [n for n in official_results if n.get("alive")]
    print(f"[3/5] 最新节点检测完成：存活 {len(alive_official)} / {len(candidates)} 个")

    post_json("/admin/api/save-current-pool", {"nodes": alive_official, "fetched": len(candidates)})
    print("      ✓ 当前最新节点已更新！")

    print("[4/5] 检查历史留存节点...")
    history_data = get_json("/admin/api/prepare-history")
    history_candidates = history_data.get("candidates", [])
    print(f"      待复测历史节点: {len(history_candidates)} 个")

    alive_history = []
    dead_history_keys = []

    if history_candidates:
        history_results = check_in_chunks(history_candidates, BATCH_SIZE)
        alive_history = [n for n in history_results if n.get("alive")]
        dead_history_keys = [f"{n['host'].lower()}:{n['port']}" for n in history_results if not n.get("alive")]

    print(f"[5/5] 更新历史节点: 保留 {len(alive_history)} 个，移除失效 {len(dead_history_keys)} 个")
    post_json("/admin/api/clean-history-pool", {
        "aliveHistoryNodes": alive_history,
        "deadKeys": dead_history_keys
    })

    print("完成: 数据已全部同步至CF Workers")

if __name__ == "__main__":
    main()