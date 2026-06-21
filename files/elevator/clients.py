"""
clients.py — 客戶端程式

包含三個角色：
  run_passenger_client() — 乘客呼叫面板（互動式 CLI）
  run_security_monitor() — 安全監控中心（即時刷新面板）
  run_demo()             — 一鍵展示模式（單一終端，不需多視窗）
"""

import asyncio
import json
import logging
import sys

from .config import SERVER_HOST, SERVER_PORT
from .server import ElevatorServer


# =====================================================================
# 乘客呼叫面板
# =====================================================================
async def run_passenger_client(
    host: str = SERVER_HOST,
    port: int = SERVER_PORT,
) -> None:
    try:
        reader, writer = await asyncio.open_connection(host, port)
    except ConnectionRefusedError:
        logging.error("無法連線，請確認 server 是否已啟動。")
        return

    print("\n--- 乘客呼叫面板 (1F - 10F) ---")
    print("輸入格式: [所在樓層] [目標樓層]  （例: 2 9），輸入 exit 退出")

    loop = asyncio.get_running_loop()
    try:
        while True:
            user_input = await loop.run_in_executor(None, input, "發送呼叫: ")
            if user_input.strip().lower() == "exit":
                break
            try:
                cur, dest = map(int, user_input.strip().split())
                payload = {"type": "hall_call", "current": cur, "destination": dest}
                writer.write((json.dumps(payload) + "\n").encode())
                await writer.drain()
            except ValueError:
                print("格式錯誤：請輸入兩個 1~10 的整數。")
    finally:
        writer.close()
        await writer.wait_closed()


# =====================================================================
# 安全監控中心
# =====================================================================
async def run_security_monitor(
    host: str = SERVER_HOST,
    port: int = SERVER_PORT,
) -> None:
    try:
        reader, writer = await asyncio.open_connection(host, port)
    except ConnectionRefusedError:
        logging.error("監控中心連線失敗，請確認 server 是否正常運作。")
        return

    print("--- 安全監控中心即時面板已上線 ---")
    try:
        # 握手：告訴 server 這是 monitor 連線，server 會立刻推一次當前狀態
        writer.write((json.dumps({"type": "monitor"}) + "\n").encode())
        await writer.drain()

        update_count = 0
        while True:
            data = await reader.readline()
            if not data:
                print("--- 連線已中斷 ---")
                break
            msg = json.loads(data.decode().strip())
            if msg.get("type") != "status_update":
                continue

            update_count += 1
            parts = [
                f"{el['id'].upper()}: {el['current_floor']:2d}F "
                f"({el['direction']:<4s}) 停靠{el['targets']}"
                for el in msg["elevators"]
            ]
            line = f"[更新 #{update_count:03d}] " + "  |  ".join(parts)
            sys.stdout.write(line + "\n")
            sys.stdout.flush()

    except asyncio.CancelledError:
        pass
    finally:
        writer.close()
        await writer.wait_closed()


# =====================================================================
# Demo 模式（單一終端）
# =====================================================================
async def run_demo(
    host: str = SERVER_HOST,
    demo_port: int = 8889,
) -> None:
    """
    在同一個 event loop 內啟動 server、monitor、自動乘客，
    讓評審一鍵看到兩台電梯同時移動。
    """
    print("\n" + "=" * 60)
    print("  DEMO 模式：兩台電梯並發展示（單一終端）")
    print("=" * 60)

    server = ElevatorServer(host=host, port=demo_port)

    async def _server_bg() -> None:
        try:
            await server.start()
        except asyncio.CancelledError:
            pass

    server_task = asyncio.create_task(_server_bg())
    await asyncio.sleep(0.3)   # 等 socket 就緒

    async def _inline_monitor() -> None:
        try:
            reader, writer = await asyncio.open_connection(host, demo_port)
            writer.write((json.dumps({"type": "monitor"}) + "\n").encode())
            await writer.drain()
            while True:
                line = await reader.readline()
                if not line:
                    break
                msg = json.loads(line.decode().strip())
                if msg.get("type") == "status_update":
                    parts = [
                        f"{el['id'].upper()}: {el['current_floor']:2d}F "
                        f"({el['direction']:<4s}) 停靠{el['targets']}"
                        for el in msg["elevators"]
                    ]
                    sys.stdout.write("  |  ".join(parts) + "\n")
                    sys.stdout.flush()
        except asyncio.CancelledError:
            pass

    monitor_task = asyncio.create_task(_inline_monitor())
    await asyncio.sleep(0.2)

    try:
        _, writer = await asyncio.open_connection(host, demo_port)

        print("\n[Demo] 同時送出：1F→10F 與 8F→2F\n")
        for payload in [
            {"type": "hall_call", "current": 1, "destination": 10},
            {"type": "hall_call", "current": 8, "destination": 2},
        ]:
            writer.write((json.dumps(payload) + "\n").encode())
        await writer.drain()

        await asyncio.sleep(4)
        print("\n[Demo] 插入順路請求：5F→9F\n")
        writer.write(
            (json.dumps({"type": "hall_call", "current": 5, "destination": 9}) + "\n").encode()
        )
        await writer.drain()

        await asyncio.sleep(8)
        writer.close()
        await writer.wait_closed()

    except Exception as exc:
        print(f"Demo 乘客異常: {exc}")

    print("\n  DEMO 結束，回收資源...")
    monitor_task.cancel()
    server_task.cancel()
    await asyncio.gather(monitor_task, server_task, return_exceptions=True)
    await server.shutdown()
