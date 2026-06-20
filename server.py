"""
server.py — 中央控制伺服器

職責：
  - 接受 TCP 連線（乘客 hall_call + 監控 monitor）
  - 驗證輸入、派車（兩段式：先接人再送人）
  - 廣播電梯狀態至所有監控端（broadcast_worker 與物理迴圈完全解耦）
  - Graceful Shutdown：安全取消所有 asyncio.Task
"""

import asyncio
import json
import logging
from typing import List, Optional, Set

from .config import SERVER_HOST, SERVER_PORT, TOTAL_FLOORS
from .dispatcher import ElevatorDispatcher
from .elevator import Elevator


class ElevatorServer:
    """
    廣播架構：
      每台電梯有獨立的 broadcast_queue。
      _broadcast_worker Task 消化佇列並推播給所有監控端。
      電梯物理迴圈不知道 TCP 的存在，不受任何 client 的網路狀況影響。
    """

    def __init__(
        self,
        host: str = SERVER_HOST,
        port: int = SERVER_PORT,
        num_elevators: int = 2,
    ):
        self.host = host
        self.port = port
        self.elevators: List[Elevator] = [
            Elevator(f"elevator{i + 1}", current_floor=1)
            for i in range(num_elevators)
        ]
        self.dispatcher     = ElevatorDispatcher(self.elevators)
        self.clients: Set[asyncio.StreamWriter] = set()
        self.broadcast_queue: asyncio.Queue = asyncio.Queue()  # 所有電梯共用
        self.lifecycle_tasks: List[asyncio.Task] = []
        self.broadcast_task: Optional[asyncio.Task] = None
        self.server         = None
        self._shutdown_called = False

    # ------------------------------------------------------------------
    # 兩段式派車
    # ------------------------------------------------------------------
    async def _dispatch_trip(self, elevator: Elevator, pickup: int, dest: int) -> None:
        """
        Phase 1：把電梯送到 pickup（乘客所在樓層）。
        Phase 2：確認停靠完成後，才把 dest 排入目標。

        這樣可確保 LOOK 不會在乘客上車前就路過並停靠目的地樓層。

        add_target() 只更新記憶體中的目標清單，不會主動廣播；
        在每個階段設定完目標後，立刻丟一個訊號進 broadcast_queue，
        讓 monitor 在電梯真正開始移動前就能看到「已規劃停靠點」，
        不必等到下一次樓層變化才更新畫面。
        """
        await elevator.add_target(pickup)
        await self.broadcast_queue.put(1)   # 立即刷新：顯示已規劃前往 pickup

        while True:
            async with elevator.lock:
                arrived = (
                    elevator.current_floor == pickup
                    and pickup not in elevator.targets_set
                )
            if arrived:
                break
            await asyncio.sleep(0.1)

        await elevator.add_target(dest)
        await self.broadcast_queue.put(1)   # 立即刷新：顯示已規劃前往 dest

    # ------------------------------------------------------------------
    # 廣播
    # ------------------------------------------------------------------
    async def _push_to_all_clients(self, payload: bytes) -> None:
        """向所有 client 推播，移除已斷線者。"""
        disconnected: Set[asyncio.StreamWriter] = set()
        for writer in self.clients:
            try:
                writer.write(payload)
                await writer.drain()
            except (ConnectionError, RuntimeError):
                disconnected.add(writer)
        self.clients -= disconnected

    async def _broadcast_worker(self) -> None:
        """
        單一 worker，消化共用 broadcast_queue。
        任何電梯有狀態變化時往 queue 丟一個訊號，worker 就組裝
        所有電梯的當前快照並推播給所有 monitor，確保面板永遠看到全局狀態。
        """
        try:
            while True:
                await self.broadcast_queue.get()

                # 排空 queue，避免短時間內連發大量廣播
                while not self.broadcast_queue.empty():
                    self.broadcast_queue.get_nowait()

                elevators_data = []
                for e in self.elevators:
                    s = await e.get_state_snapshot()
                    elevators_data.append({
                        "id":            e.elevator_id,
                        "current_floor": s["current_floor"],
                        "direction":     s["direction"],
                        "targets":       s["targets"],
                    })

                payload = (
                    json.dumps({"type": "status_update", "elevators": elevators_data}) + "\n"
                ).encode()
                await self._push_to_all_clients(payload)

        except asyncio.CancelledError:
            raise

    # ------------------------------------------------------------------
    # 客戶端處理
    # ------------------------------------------------------------------
    async def _send_snapshot(self, writer: asyncio.StreamWriter) -> None:
        """立刻把所有電梯的目前狀態推給指定 writer（monitor 上線時用）。"""
        elevators_data = []
        for e in self.elevators:
            s = await e.get_state_snapshot()
            elevators_data.append({
                "id":            e.elevator_id,
                "current_floor": s["current_floor"],
                "direction":     s["direction"],
                "targets":       s["targets"],
            })
        payload = (
            json.dumps({"type": "status_update", "elevators": elevators_data}) + "\n"
        ).encode()
        try:
            writer.write(payload)
            await writer.drain()
        except (ConnectionError, RuntimeError):
            pass

    async def handle_client(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        self.clients.add(writer)
        try:
            while True:
                data = await reader.readline()
                if not data:
                    break

                try:
                    request = json.loads(data.decode().strip())
                except json.JSONDecodeError as exc:
                    logging.warning(f"非合規 JSON: {exc}")
                    continue

                msg_type = request.get("type")

                if msg_type == "monitor":
                    # Monitor 上線：立刻推一次當前全狀態，讓面板不用等電梯移動
                    await self._send_snapshot(writer)
                    continue

                if msg_type != "hall_call":
                    continue

                try:
                    pickup = int(request["current"])
                    dest   = int(request["destination"])
                except (ValueError, TypeError, KeyError) as exc:
                    logging.warning(f"欄位錯誤: {exc}")
                    continue

                if not (1 <= pickup <= TOTAL_FLOORS and 1 <= dest <= TOTAL_FLOORS):
                    logging.warning(f"越界拒絕: {pickup}F -> {dest}F")
                    continue
                if pickup == dest:
                    logging.warning(f"同樓層無效請求: {pickup}F")
                    continue

                best = await self.dispatcher.select_best_elevator(pickup, dest)
                asyncio.create_task(self._dispatch_trip(best, pickup, dest))

        except (ConnectionResetError, BrokenPipeError):
            pass
        finally:
            self.clients.discard(writer)
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass

    # ------------------------------------------------------------------
    # 生命週期
    # ------------------------------------------------------------------
    async def start(self) -> None:
        self.server = await asyncio.start_server(self.handle_client, self.host, self.port)
        logging.info(f"電梯調度主機已於 {self.host}:{self.port} 啟動。")

        # 所有電梯共用同一個 broadcast_queue
        self.lifecycle_tasks = [
            asyncio.create_task(el.run_lifecycle(self.broadcast_queue))
            for el in self.elevators
        ]
        # 單一 worker 負責廣播完整全局狀態
        self.broadcast_task = asyncio.create_task(self._broadcast_worker())

        try:
            async with self.server:
                await self.server.serve_forever()
        except asyncio.CancelledError:
            logging.info("收到終止訊號，開始安全清理...")
        finally:
            await self.shutdown()

    async def shutdown(self) -> None:
        if self._shutdown_called:
            return
        self._shutdown_called = True

        all_tasks = self.lifecycle_tasks[:]
        if self.broadcast_task:
            all_tasks.append(self.broadcast_task)
        for task in all_tasks:
            task.cancel()
        await asyncio.gather(*all_tasks, return_exceptions=True)

        for writer in list(self.clients):
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

        logging.info("資源清理完畢。")
