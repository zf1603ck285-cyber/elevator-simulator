"""
電梯模擬器 v3 — 終版
======================
對照 v2 評語的三個致命傷與一個次要瑕疵，全部修正：

  [FIX-A] 廣播解耦：asyncio.Queue + 獨立 worker，電梯不再等 TCP
  [FIX-B] move() 介面誠實化：current_floor ← current，同時 add_target(floor)
  [FIX-C] IDLE 同樓層邊界：nearest == curr_floor 時維持 IDLE，不亂閃方向燈
  [FIX-D] Server 驗證測試改為真實 Socket 整合測試
"""

import asyncio
import json
import os
import random
import sys
import logging
import unittest
from typing import List, Set, Optional

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

TOTAL_FLOORS     = 10
MOVE_SECONDS     = 1.0
DOOR_OPEN_SECONDS= 0.5
COST_DOOR_PENALTY   = 2.0
COST_DETOUR_PENALTY = 15.0
COST_ON_WAY_DISCOUNT= 0.8


# =====================================================================
# 1. 電梯核心模組
# =====================================================================
class Elevator:
    """
    單一電梯實體。
    ─ 物理狀態由 run_lifecycle 驅動，以 asyncio.Lock 保護。
    ─ 廣播完全解耦：run_lifecycle 只把快照丟入 broadcast_queue，
      不直接呼叫任何 I/O，消除 TCP backpressure 致命傷。[FIX-A]
    """

    def __init__(self, elevator_id: str, current_floor: int = 1):
        self.elevator_id: str = elevator_id
        self.current_floor: int = current_floor
        self.direction: str = "IDLE"
        self.targets: List[int] = []
        self.targets_set: Set[int] = set()
        self.lock: asyncio.Lock = asyncio.Lock()
        # [FIX-A] 廣播佇列：電梯只寫入，Server worker 負責消化
        self.broadcast_queue: asyncio.Queue = asyncio.Queue()

    # ------------------------------------------------------------------
    # 題目要求介面 [FIX-B]
    # ------------------------------------------------------------------
    def move(self, current: int, floor: int) -> None:
        """
        [FIX-B] 題目規定的同步介面，兩個參數都有語意：
          current → 立即同步覆寫電梯的物理位置（例：系統初始化定位）
          floor   → 排入 LOOK 目標隊列（非同步執行由 run_lifecycle 完成）

        注意：因為 add_target 是 async，這裡用 asyncio.ensure_future 把它
        安排進 event loop，呼叫端不需要 await。
        若需要確保 floor 已加入再繼續，請改 await elevator.add_target(floor)。
        """
        self.current_floor = current
        self.display_floor()
        asyncio.ensure_future(self.add_target(floor))

    def display_floor(self) -> str:
        """格式化目前狀態字串，同時印出並回傳。"""
        status = (
            f"[{self.elevator_id.upper()}] "
            f"目前樓層: {self.current_floor:2d}F | "
            f"方向: {self.direction:<4s} | "
            f"規劃停靠: {self.targets}"
        )
        print(status)
        return status

    # ------------------------------------------------------------------
    # 並發安全狀態操作
    # ------------------------------------------------------------------
    async def get_state_snapshot(self) -> dict:
        async with self.lock:
            return {
                "current_floor": self.current_floor,
                "direction": self.direction,
                "targets": list(self.targets),
            }

    async def add_target(self, floor: int) -> None:
        async with self.lock:
            if floor not in self.targets_set:
                self.targets_set.add(floor)
                self.targets.append(floor)
                self.targets.sort()
                if self.direction == "IDLE" and floor != self.current_floor:
                    self.direction = "UP" if floor > self.current_floor else "DOWN"

    # ------------------------------------------------------------------
    # LOOK 核心子方法
    # ------------------------------------------------------------------
    def _decide_next(self, curr_floor: int) -> Optional[int]:
        """
        純記憶體決策，呼叫前必須持有 self.lock。
        [FIX-C] IDLE 且 nearest == curr_floor 時不修改 direction，
                避免方向燈瞬間亮 DOWN 的狀態機汙染。
        """
        if not self.targets:
            self.direction = "IDLE"
            return None

        up_c   = [t for t in self.targets if t > curr_floor]
        down_c = [t for t in self.targets if t < curr_floor]

        if self.direction == "UP":
            if up_c:   return min(up_c)
            if down_c: self.direction = "DOWN"; return max(down_c)

        elif self.direction == "DOWN":
            if down_c: return max(down_c)
            if up_c:   self.direction = "UP";   return min(up_c)

        else:  # IDLE
            nearest = min(self.targets, key=lambda t: abs(t - curr_floor))
            if nearest == curr_floor:          # [FIX-C] 已在目標樓層，保持 IDLE
                return nearest                 # 讓 should_stop 在下一輪處理它
            self.direction = "UP" if nearest > curr_floor else "DOWN"
            return nearest

        self.direction = "IDLE"
        return None

    def _step_floor(self) -> None:
        if self.direction == "UP":
            self.current_floor += 1
        elif self.direction == "DOWN":
            self.current_floor -= 1

    # ------------------------------------------------------------------
    # LOOK 生命週期 [FIX-A]
    # ------------------------------------------------------------------
    async def run_lifecycle(self) -> None:
        """
        [FIX-A] broadcast_callback 已完全移除。
        電梯只把快照丟入 self.broadcast_queue，不直接接觸任何 Socket I/O。
        一個慢 client 再也無法讓電梯卡住。
        """
        try:
            while True:
                await asyncio.sleep(0.05)

                should_stop = False
                next_dest: Optional[int] = None
                curr_floor: int = 0

                async with self.lock:
                    curr_floor = self.current_floor
                    if curr_floor in self.targets_set:
                        self.targets_set.remove(curr_floor)
                        self.targets.remove(curr_floor)
                        should_stop = True
                    next_dest = self._decide_next(curr_floor)

                if should_stop:
                    logging.info(f"[{self.elevator_id.upper()}] 停靠 {curr_floor}F，開門。")
                    await self.broadcast_queue.put(await self.get_state_snapshot())
                    await asyncio.sleep(DOOR_OPEN_SECONDS)
                    if next_dest is None:
                        continue

                if next_dest is not None:
                    await asyncio.sleep(MOVE_SECONDS)
                    async with self.lock:
                        self._step_floor()
                    logging.info(self.display_floor())
                    await self.broadcast_queue.put(await self.get_state_snapshot())

        except asyncio.CancelledError:
            logging.debug(f"[{self.elevator_id.upper()}] 生命週期任務已安全取消。")
            raise


# =====================================================================
# 2. 調度器
# =====================================================================
class ElevatorDispatcher:

    def __init__(self, elevators: List[Elevator]):
        self.elevators = elevators

    def calculate_cost(self, snapshot: dict, pickup: int, destination: int) -> float:
        curr      = snapshot["current_floor"]
        direction = snapshot["direction"]
        targets   = snapshot["targets"]

        distance     = abs(curr - pickup)
        request_dir  = "UP" if destination > pickup else "DOWN"
        base_cost    = distance * MOVE_SECONDS + len(targets) * COST_DOOR_PENALTY

        if direction == "IDLE":
            return base_cost

        if direction == request_dir:
            on_way = (direction == "UP" and curr <= pickup) or \
                     (direction == "DOWN" and curr >= pickup)
            if on_way:
                return base_cost * COST_ON_WAY_DISCOUNT

        return base_cost + COST_DETOUR_PENALTY

    async def select_best_elevator(self, pickup: int, destination: int) -> Elevator:
        costs = []
        for e in self.elevators:
            snap = await e.get_state_snapshot()
            costs.append((self.calculate_cost(snap, pickup, destination), e))
        min_cost   = min(c for c, _ in costs)
        candidates = [e for c, e in costs if c == min_cost]
        return random.choice(candidates)


# =====================================================================
# 3. 中央控制伺服器
# =====================================================================
class ElevatorServer:
    """
    [FIX-A] 廣播架構重構：
    ─ 每台電梯有自己的 broadcast_queue
    ─ 獨立的 _broadcast_worker Task 消化各電梯的佇列
    ─ run_lifecycle 與 TCP I/O 完全解耦
    """

    def __init__(self, host: str = "127.0.0.1", port: int = 8888, num_elevators: int = 2):
        self.host = host
        self.port = port
        self.elevators = [
            Elevator(f"elevator{i + 1}", current_floor=1) for i in range(num_elevators)
        ]
        self.dispatcher     = ElevatorDispatcher(self.elevators)
        self.clients: Set[asyncio.StreamWriter] = set()
        self.lifecycle_tasks: List[asyncio.Task] = []
        self.broadcast_tasks: List[asyncio.Task] = []
        self.server = None
        self._shutdown_called = False   # guard：防止 shutdown() 被重複執行

    async def _push_to_all_clients(self, payload: bytes) -> None:
        """向所有 client 送出 payload，踢除已斷線者。不 await drain 以外的耗時操作。"""
        disconnected: Set[asyncio.StreamWriter] = set()
        for writer in self.clients:
            try:
                writer.write(payload)
                await writer.drain()
            except (ConnectionError, RuntimeError):
                disconnected.add(writer)
        self.clients -= disconnected

    async def _broadcast_worker(self, elevator: Elevator) -> None:
        """
        [FIX-A] 獨立 Task，持續從電梯的 broadcast_queue 取快照並推播。
        電梯物理迴圈完全不知道 TCP 的存在。
        """
        try:
            while True:
                snap = await elevator.broadcast_queue.get()
                elevators_data = []
                for e in self.elevators:
                    if e is elevator:
                        elevators_data.append({
                            "id": e.elevator_id,
                            "current_floor": snap["current_floor"],
                            "direction": snap["direction"],
                            "targets": snap["targets"],
                        })
                    else:
                        s = await e.get_state_snapshot()
                        elevators_data.append({
                            "id": e.elevator_id,
                            "current_floor": s["current_floor"],
                            "direction": s["direction"],
                            "targets": s["targets"],
                        })
                payload = (
                    json.dumps({"type": "status_update", "elevators": elevators_data}) + "\n"
                ).encode()
                await self._push_to_all_clients(payload)
        except asyncio.CancelledError:
            raise

    async def handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
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

                if request.get("type") != "hall_call":
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
                await best.add_target(pickup)
                await best.add_target(dest)

        except (ConnectionResetError, BrokenPipeError):
            pass
        finally:
            self.clients.discard(writer)
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass

    async def start(self) -> None:
        self.server = await asyncio.start_server(self.handle_client, self.host, self.port)
        logging.info(f"電梯調度主機已於 {self.host}:{self.port} 啟動。")

        # 電梯物理迴圈
        self.lifecycle_tasks = [
            asyncio.create_task(el.run_lifecycle())
            for el in self.elevators
        ]
        # 廣播 worker（每台電梯一個，完全獨立於物理迴圈）
        self.broadcast_tasks = [
            asyncio.create_task(self._broadcast_worker(el))
            for el in self.elevators
        ]

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
        all_tasks = self.lifecycle_tasks + self.broadcast_tasks
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


# =====================================================================
# 4. 客戶端
# =====================================================================
async def run_passenger_client() -> None:
    try:
        reader, writer = await asyncio.open_connection("127.0.0.1", 8888)
    except ConnectionRefusedError:
        logging.error("無法連線，請確認 server 是否已啟動。")
        return

    print("\n--- 乘客呼叫面板 (1F - 10F) ---")
    print("輸入格式: [所在樓層] [目標樓層]  (例: 2 9)，輸入 exit 退出")

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


async def run_security_monitor() -> None:
    try:
        reader, writer = await asyncio.open_connection("127.0.0.1", 8888)
    except ConnectionRefusedError:
        logging.error("監控中心連線失敗，請確認 server 是否正常運作。")
        return

    print("--- 安全監控中心即時面板已上線 ---")
    try:
        while True:
            data = await reader.readline()
            if not data:
                break
            msg = json.loads(data.decode().strip())
            if msg.get("type") == "status_update":
                os.system("cls" if os.name == "nt" else "clear")
                print("=" * 63)
                print("          監控中心：電梯動態 LOOK 排程面板")
                print("=" * 63)
                for el in msg["elevators"]:
                    track = [" . "] * TOTAL_FLOORS
                    track[el["current_floor"] - 1] = f"[{el['id'].upper()}]"
                    track_str = " | ".join(reversed(track))
                    print(f"\n軌道 ({el['id'].upper()}): 10F -> | {track_str} | -> 1F")
                    print(
                        f"狀態: {el['current_floor']:2d}F | "
                        f"{el['direction']:<4s} | 停靠: {el['targets']}"
                    )
                print("=" * 63)
    except asyncio.CancelledError:
        pass
    finally:
        writer.close()
        await writer.wait_closed()


# =====================================================================
# 5. Demo 模式（單一終端）
# =====================================================================
async def run_demo() -> None:
    port = 8889
    print("\n" + "=" * 60)
    print("  DEMO 模式：兩台電梯並發展示（單一終端）")
    print("=" * 60)

    server = ElevatorServer(host="127.0.0.1", port=port)

    async def _server_bg():
        try:
            await server.start()
        except asyncio.CancelledError:
            pass

    server_task = asyncio.create_task(_server_bg())
    await asyncio.sleep(0.3)

    async def _inline_monitor():
        try:
            reader, _ = await asyncio.open_connection("127.0.0.1", port)
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
                    print("  |  ".join(parts))
        except asyncio.CancelledError:
            pass

    monitor_task = asyncio.create_task(_inline_monitor())
    await asyncio.sleep(0.2)

    try:
        _, writer = await asyncio.open_connection("127.0.0.1", port)
        print("\n[Demo] 同時送出：1F→10F 與 8F→2F\n")
        for payload in [
            {"type": "hall_call", "current": 1, "destination": 10},
            {"type": "hall_call", "current": 8, "destination": 2},
        ]:
            writer.write((json.dumps(payload) + "\n").encode())
        await writer.drain()

        await asyncio.sleep(4)
        print("\n[Demo] 插入順路請求：5F→9F\n")
        writer.write((json.dumps({"type": "hall_call", "current": 5, "destination": 9}) + "\n").encode())
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


# =====================================================================
# 6. 單元測試
# =====================================================================
TEST_PORT = 18888  # 測試專用埠，避免衝突


class TestElevatorSystem(unittest.IsolatedAsyncioTestCase):

    # ── 物理層 ──

    async def test_step_floor_up_and_down(self):
        el = Elevator("t1", current_floor=5)
        el.direction = "UP";  el._step_floor(); self.assertEqual(el.current_floor, 6)
        el.direction = "DOWN"; el._step_floor(); self.assertEqual(el.current_floor, 5)

    async def test_request_deduplication(self):
        el = Elevator("t2", current_floor=1)
        for _ in range(3):
            await el.add_target(7)
        snap = await el.get_state_snapshot()
        self.assertEqual(snap["targets"], [7])

    async def test_look_initial_direction_stays_up(self):
        el = Elevator("t3", current_floor=5)
        el.direction = "UP"
        await el.add_target(10)
        await el.add_target(1)
        self.assertEqual((await el.get_state_snapshot())["direction"], "UP")

    async def test_look_reversal_correct_terminal(self):
        """電梯從 5F UP 出發，依序停 10F 後反轉，真正抵達 1F 才算通過。"""
        el = Elevator("t4", current_floor=5)
        el.direction = "UP"
        await el.add_target(10)
        await el.add_target(1)

        task = asyncio.create_task(el.run_lifecycle())
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
        """[FIX-C] IDLE 時選最近鄰（9F），不選 targets[0]（2F）。"""
        el = Elevator("t5", current_floor=8)
        async with el.lock:
            el.targets = [2, 9]
            el.targets_set = {2, 9}
        self.assertEqual(el._decide_next(8), 9)

    async def test_idle_same_floor_no_direction_flip(self):
        """[FIX-C] IDLE 且目標 == 當前樓層，direction 不應變成 DOWN。"""
        el = Elevator("t6", current_floor=5)
        async with el.lock:
            el.targets = [5]
            el.targets_set = {5}
        el._decide_next(5)
        self.assertNotEqual(el.direction, "DOWN", "同樓層不應觸發 DOWN 方向燈")

    # ── 調度層 ──

    async def test_dispatcher_idle_cheaper_than_busy(self):
        idle = Elevator("idle", current_floor=1)
        busy = Elevator("busy", current_floor=1)
        for f in [2, 3, 4, 5, 6]:
            await busy.add_target(f)
        d = ElevatorDispatcher([idle, busy])
        idle_cost = d.calculate_cost(await idle.get_state_snapshot(), 7, 10)
        busy_cost = d.calculate_cost(await busy.get_state_snapshot(), 7, 10)
        self.assertLess(idle_cost, busy_cost)

    # ── move() 介面 [FIX-B] ──

    async def test_move_updates_current_floor(self):
        """move(current, floor) 必須立即更新 current_floor。"""
        el = Elevator("t7", current_floor=1)
        # 需要一個 running event loop 讓 ensure_future 能排隊
        el.move(5, 8)
        self.assertEqual(el.current_floor, 5)

    async def test_move_enqueues_floor_target(self):
        """move(current, floor) 的 floor 參數必須真正進入目標佇列。"""
        el = Elevator("t8", current_floor=1)
        el.move(3, 7)
        await asyncio.sleep(0)   # 讓 ensure_future 的 add_target 執行
        snap = await el.get_state_snapshot()
        self.assertIn(7, snap["targets"], "floor=7 應出現在 targets 中")

    # ── Server 整合測試 [FIX-D] ──

    async def _start_test_server(self) -> ElevatorServer:
        """啟動一個使用真實 Socket 的測試 Server，回傳實體。"""
        srv = ElevatorServer(host="127.0.0.1", port=TEST_PORT, num_elevators=2)
        self._server_task = asyncio.create_task(srv.start())
        await asyncio.sleep(0.2)   # 等 socket 就緒
        return srv

    async def _stop_test_server(self, srv: ElevatorServer) -> None:
        self._server_task.cancel()
        await asyncio.gather(self._server_task, return_exceptions=True)
        await srv.shutdown()

    async def test_server_rejects_out_of_range_via_socket(self):
        """
        [FIX-D] 真實整合測試：透過 Socket 送越界請求，
        驗證所有電梯的 targets 都沒有改變。
        """
        srv = await self._start_test_server()
        try:
            _, writer = await asyncio.open_connection("127.0.0.1", TEST_PORT)
            bad = json.dumps({"type": "hall_call", "current": 0, "destination": 99}) + "\n"
            writer.write(bad.encode())
            await writer.drain()
            await asyncio.sleep(0.1)   # 給 server 時間處理

            for el in srv.elevators:
                snap = await el.get_state_snapshot()
                self.assertEqual(snap["targets"], [], f"{el.elevator_id} 不應有任何目標")

            writer.close()
            await writer.wait_closed()
        finally:
            await self._stop_test_server(srv)

    async def test_server_rejects_same_floor_via_socket(self):
        """[FIX-D] 同樓層請求（5→5）透過 Socket 送出，targets 應保持空。"""
        srv = await self._start_test_server()
        try:
            _, writer = await asyncio.open_connection("127.0.0.1", TEST_PORT)
            bad = json.dumps({"type": "hall_call", "current": 5, "destination": 5}) + "\n"
            writer.write(bad.encode())
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
        """[FIX-D] 合法請求（2→9）應讓某台電梯的 targets 非空。"""
        srv = await self._start_test_server()
        try:
            _, writer = await asyncio.open_connection("127.0.0.1", TEST_PORT)
            good = json.dumps({"type": "hall_call", "current": 2, "destination": 9}) + "\n"
            writer.write(good.encode())
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


# =====================================================================
# 7. 系統進入點
# =====================================================================
def _apply_windows_selector_policy() -> None:
    """
    Windows + Python 3.9 的已知問題：
    ProactorEventLoop 的 _ProactorBasePipeTransport.__del__ 會在 GC 時
    嘗試呼叫已關閉的 event loop，噴出 RuntimeError: Event loop is closed。
    這是 CPython bpo-39232，Python 3.10 才修復。

    解法：在 Windows 上改用 SelectorEventLoop，它的 __del__ 不依賴
    running loop，完全迴避這個問題。TCP Server 功能不受影響。
    """
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


if __name__ == "__main__":
    _apply_windows_selector_policy()

    if len(sys.argv) > 1:
        mode = sys.argv[1].lower()
        if mode == "server":
            srv = ElevatorServer()
            try:
                asyncio.run(srv.start())
            except KeyboardInterrupt:
                logging.info("強制終止。")
        elif mode == "passenger":
            asyncio.run(run_passenger_client())
        elif mode == "monitor":
            asyncio.run(run_security_monitor())
        elif mode == "demo":
            asyncio.run(run_demo())
        elif mode == "test":
            unittest.main(argv=[sys.argv[0]])
        else:
            print(f"未知模式: {mode}")
    else:
        print("使用方式：")
        print("  python ElevatorSimulator_v3.py demo       # 一鍵展示（推薦）")
        print("  python ElevatorSimulator_v3.py server     # 啟動控制主機")
        print("  python ElevatorSimulator_v3.py monitor    # 啟動監控面板")
        print("  python ElevatorSimulator_v3.py passenger  # 啟動乘客端")
        print("  python ElevatorSimulator_v3.py test       # 執行單元測試")