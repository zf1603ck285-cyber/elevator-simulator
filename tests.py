"""
tests.py — 單元測試 + Server 整合測試

執行：
  python -m pytest elevator/tests.py -v
  python main.py test
"""

import asyncio
import json
import unittest

from .dispatcher import ElevatorDispatcher
from .elevator import Elevator
from .server import ElevatorServer

TEST_PORT = 18888   # 測試專用埠，避免與開發 server 衝突


class TestElevatorSystem(unittest.IsolatedAsyncioTestCase):

    # ── 物理層 ──────────────────────────────────────────────────────

    async def test_step_floor_up_and_down(self):
        """_step_floor 在 UP / DOWN 方向各移動一層。"""
        el = Elevator("t1", current_floor=5)
        el.direction = "UP";   el._step_floor(); self.assertEqual(el.current_floor, 6)
        el.direction = "DOWN"; el._step_floor(); self.assertEqual(el.current_floor, 5)

    async def test_request_deduplication(self):
        """相同目標加入多次，內部只保留一筆。"""
        el = Elevator("t2", current_floor=1)
        for _ in range(3):
            await el.add_target(7)
        snap = await el.get_state_snapshot()
        self.assertEqual(snap["targets"], [7])

    async def test_look_initial_direction_stays_up(self):
        """UP 方向：先完成高樓層目標，才反轉向下。"""
        el = Elevator("t3", current_floor=5)
        el.direction = "UP"
        await el.add_target(10)
        await el.add_target(1)
        self.assertEqual((await el.get_state_snapshot())["direction"], "UP")

    async def test_look_reversal_correct_terminal(self):
        """電梯從 5F UP 出發，停 10F 後反轉，真正抵達 1F 且 IDLE 才通過。"""
        el = Elevator("t4", current_floor=5)
        el.direction = "UP"
        await el.add_target(10)
        await el.add_target(1)

        q = asyncio.Queue()
        task = asyncio.create_task(el.run_lifecycle(q))
        for _ in range(600):
            await asyncio.sleep(0.05)
            async with el.lock:
                if el.current_floor == 1 and not el.targets:
                    break
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        self.assertEqual(el.current_floor, 1)
        self.assertEqual(el.direction, "IDLE")

    async def test_idle_nearest_neighbour(self):
        """IDLE 時選最近鄰（9F），不選 targets[0] 的最小值（2F）。"""
        el = Elevator("t5", current_floor=8)
        async with el.lock:
            el.targets     = [2, 9]
            el.targets_set = {2, 9}
        self.assertEqual(el._decide_next(8), 9)

    async def test_idle_same_floor_no_direction_flip(self):
        """IDLE 且目標 == 當前樓層，direction 不應變成 DOWN。"""
        el = Elevator("t6", current_floor=5)
        async with el.lock:
            el.targets     = [5]
            el.targets_set = {5}
        el._decide_next(5)
        self.assertNotEqual(el.direction, "DOWN", "同樓層不應觸發 DOWN 方向燈")

    # ── 調度層 ──────────────────────────────────────────────────────

    async def test_dispatcher_idle_cheaper_than_busy(self):
        """IDLE 電梯的成本低於已有多個停靠點的電梯。"""
        idle = Elevator("idle", current_floor=1)
        busy = Elevator("busy", current_floor=1)
        for f in [2, 3, 4, 5, 6]:
            await busy.add_target(f)
        d = ElevatorDispatcher([idle, busy])
        idle_cost = d.calculate_cost(await idle.get_state_snapshot(), 7, 10)
        busy_cost = d.calculate_cost(await busy.get_state_snapshot(), 7, 10)
        self.assertLess(idle_cost, busy_cost)

    # ── move() 介面 ──────────────────────────────────────────────────

    async def test_move_updates_current_floor(self):
        """move(current, floor) 必須立即更新 current_floor。"""
        el = Elevator("t7", current_floor=1)
        el.move(5, 8)
        self.assertEqual(el.current_floor, 5)

    async def test_move_enqueues_floor_target(self):
        """move(current, floor) 的 floor 參數必須真正進入目標佇列。"""
        el = Elevator("t8", current_floor=1)
        el.move(3, 7)
        await asyncio.sleep(0)   # 讓 ensure_future 的 add_target 執行
        snap = await el.get_state_snapshot()
        self.assertIn(7, snap["targets"], "floor=7 應出現在 targets 中")

    # ── Server 整合測試（真實 Socket）────────────────────────────────

    async def _start_test_server(self) -> ElevatorServer:
        srv = ElevatorServer(host="127.0.0.1", port=TEST_PORT, num_elevators=2)
        self._server_task = asyncio.create_task(srv.start())
        await asyncio.sleep(0.2)
        return srv

    async def _stop_test_server(self, srv: ElevatorServer) -> None:
        self._server_task.cancel()
        await asyncio.gather(self._server_task, return_exceptions=True)
        await srv.shutdown()

    async def test_server_rejects_out_of_range_via_socket(self):
        """越界請求（0→99）透過 Socket 送出，所有電梯 targets 應保持空。"""
        srv = await self._start_test_server()
        try:
            _, writer = await asyncio.open_connection("127.0.0.1", TEST_PORT)
            writer.write(
                (json.dumps({"type": "hall_call", "current": 0, "destination": 99}) + "\n").encode()
            )
            await writer.drain()
            await asyncio.sleep(0.1)

            for el in srv.elevators:
                snap = await el.get_state_snapshot()
                self.assertEqual(snap["targets"], [], f"{el.elevator_id} 不應有任何目標")

            writer.close()
            await writer.wait_closed()
        finally:
            await self._stop_test_server(srv)

    async def test_server_rejects_same_floor_via_socket(self):
        """同樓層請求（5→5）透過 Socket 送出，targets 應保持空。"""
        srv = await self._start_test_server()
        try:
            _, writer = await asyncio.open_connection("127.0.0.1", TEST_PORT)
            writer.write(
                (json.dumps({"type": "hall_call", "current": 5, "destination": 5}) + "\n").encode()
            )
            await writer.drain()
            await asyncio.sleep(0.1)

            for el in srv.elevators:
                snap = await el.get_state_snapshot()
                self.assertEqual(snap["targets"], [])

            writer.close()
            await writer.wait_closed()
        finally:
            await self._stop_test_server(srv)

    async def test_server_accepts_valid_request_via_socket(self):
        """合法請求（2→9）應讓至少一台電梯的 targets 非空。"""
        srv = await self._start_test_server()
        try:
            _, writer = await asyncio.open_connection("127.0.0.1", TEST_PORT)
            writer.write(
                (json.dumps({"type": "hall_call", "current": 2, "destination": 9}) + "\n").encode()
            )
            await writer.drain()
            await asyncio.sleep(0.15)

            all_targets = []
            for el in srv.elevators:
                snap = await el.get_state_snapshot()
                all_targets.extend(snap["targets"])
            self.assertTrue(len(all_targets) > 0, "合法請求應讓至少一台電梯有目標")

            writer.close()
            await writer.wait_closed()
        finally:
            await self._stop_test_server(srv)
