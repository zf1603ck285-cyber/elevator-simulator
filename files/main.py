"""
main.py — 系統進入點

使用方式：
  python main.py demo       # 一鍵展示（推薦，單一終端）
  python main.py server     # 啟動控制主機
  python main.py monitor    # 啟動安全監控面板
  python main.py passenger  # 啟動乘客呼叫面板
  python main.py test       # 執行所有單元測試與整合測試
"""

import asyncio
import logging
import sys
import unittest

from elevator.clients import run_demo, run_passenger_client, run_security_monitor
from elevator.server import ElevatorServer


def _apply_windows_selector_policy() -> None:
    """
    Windows + Python 3.9 已知問題（CPython bpo-39232）：
    ProactorEventLoop 的 __del__ 在 GC 時會嘗試呼叫已關閉的 event loop，
    噴出 RuntimeError: Event loop is closed。Python 3.10 才修復。

    解法：改用 WindowsSelectorEventLoopPolicy，TCP Server 功能不受影響。
    """
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


def main() -> None:
    _apply_windows_selector_policy()

    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(0)

    mode = sys.argv[1].lower()

    if mode == "server":
        async def _run_server() -> None:
            # 關鍵：ElevatorServer() 必須在這裡（已有 running loop 的情況下）
            # 才能實例化。Python 3.9 的 asyncio.Lock()/asyncio.Queue() 建構子
            # 會在建構當下呼叫 get_event_loop() 並綁定迴圈；若在 asyncio.run()
            # 之外建構，綁定的會是錯誤的迴圈，導致背景 Task 操作 Queue/Lock 時
            # 跨迴圈出錯（RuntimeError，且常被吞掉而靜默失敗）。
            # Python 3.10+ 已移除這個舊行為，但這裡統一寫成「在 loop 內建構」
            # 以同時相容 3.9 與後續版本。
            srv = ElevatorServer()
            await srv.start()

        try:
            asyncio.run(_run_server())
        except KeyboardInterrupt:
            logging.info("強制終止。")

    elif mode == "passenger":
        asyncio.run(run_passenger_client())

    elif mode == "monitor":
        asyncio.run(run_security_monitor())

    elif mode == "demo":
        asyncio.run(run_demo())

    elif mode == "test":
        from elevator.tests import TestElevatorSystem
        sys.argv = [sys.argv[0]]
        loader = unittest.TestLoader()
        suite  = loader.loadTestsFromTestCase(TestElevatorSystem)
        runner = unittest.TextTestRunner(verbosity=2)
        result = runner.run(suite)
        sys.exit(0 if result.wasSuccessful() else 1)

    else:
        print(f"未知模式: {mode!r}")
        print(__doc__)
        sys.exit(1)


if __name__ == "__main__":
    main()
