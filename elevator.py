"""
elevator.py — 單一電梯實體

職責：
  - 維護物理狀態（current_floor、direction、targets）
  - 以 asyncio.Lock 保護共享狀態，避免競態
  - 實作 LOOK 排程演算法
  - 透過 broadcast_queue 通知外部狀態變化（不直接接觸 I/O）
"""

import asyncio
import logging
from typing import List, Optional, Set

from .config import DOOR_OPEN_SECONDS, MOVE_SECONDS


class Elevator:
    """
    單一電梯實體。

    廣播完全解耦：run_lifecycle 只把快照丟入 broadcast_queue，
    不直接呼叫任何 Socket I/O，確保 TCP backpressure 不會凍結電梯。
    """

    def __init__(self, elevator_id: str, current_floor: int = 1):
        self.elevator_id: str    = elevator_id
        self.current_floor: int  = current_floor
        self.direction: str      = "IDLE"
        self.targets: List[int]  = []
        self.targets_set: Set[int] = set()
        self.lock: asyncio.Lock  = asyncio.Lock()

    # ------------------------------------------------------------------
    # 題目要求介面
    # ------------------------------------------------------------------
    def move(self, current: int, floor: int) -> None:
        """
        題目規定的同步介面，兩個參數都有語意：
          current → 立即覆寫電梯的物理位置（系統初始化定位用）
          floor   → 排入 LOOK 目標隊列，由 run_lifecycle 非同步執行

        因 add_target 是 coroutine，此處用 ensure_future 排進 event loop。
        若需確保 floor 已加入再繼續，請直接 await elevator.add_target(floor)。
        """
        self.current_floor = current
        self.display_floor()
        asyncio.ensure_future(self.add_target(floor))

    def display_floor(self) -> str:
        """格式化目前狀態字串，印出並回傳。"""
        status = (
            f"[{self.elevator_id.upper()}] "
            f"目前樓層: {self.current_floor:2d}F | "
            f"方向: {self.direction:<4s} | "
            f"規劃停靠: {self.targets}"
        )
        print(status)
        return status

    # ------------------------------------------------------------------
    # 並發安全的狀態操作
    # ------------------------------------------------------------------
    async def get_state_snapshot(self) -> dict:
        """取得唯讀狀態快照，降低鎖競爭範圍。"""
        async with self.lock:
            return {
                "current_floor": self.current_floor,
                "direction":     self.direction,
                "targets":       list(self.targets),
            }

    async def add_target(self, floor: int) -> None:
        """安全地新增停靠目標，自動去重；IDLE 時同時設定初始方向。"""
        async with self.lock:
            if floor not in self.targets_set:
                self.targets_set.add(floor)
                self.targets.append(floor)
                self.targets.sort()
                if self.direction == "IDLE" and floor != self.current_floor:
                    self.direction = "UP" if floor > self.current_floor else "DOWN"

    # ------------------------------------------------------------------
    # LOOK 核心子方法（呼叫前必須持有 self.lock）
    # ------------------------------------------------------------------
    def _decide_next(self, curr_floor: int) -> Optional[int]:
        """
        LOOK 決策：依目前方向選下一個停靠目標。
        IDLE 時選最近鄰；若最近鄰恰好在當前樓層，維持 IDLE 讓
        should_stop 在下一輪處理，避免方向燈瞬間亂閃。
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
            if nearest == curr_floor:
                return nearest          # 保持 IDLE，讓 should_stop 處理
            self.direction = "UP" if nearest > curr_floor else "DOWN"
            return nearest

        self.direction = "IDLE"
        return None

    def _step_floor(self) -> None:
        """依目前方向移動一層。呼叫前必須持有 self.lock。"""
        if self.direction == "UP":
            self.current_floor += 1
        elif self.direction == "DOWN":
            self.current_floor -= 1

    # ------------------------------------------------------------------
    # LOOK 生命週期
    # ------------------------------------------------------------------
    async def run_lifecycle(self, broadcast_queue: asyncio.Queue) -> None:
        """
        LOOK 演算法主迴圈。

        設計原則：
          - 鎖內只做純記憶體運算（_decide_next / _step_floor）
          - 所有耗時操作（sleep）在鎖外執行
          - 有狀態變化時往共用 broadcast_queue 丟一個訊號（任意值），
            由 server 的單一 _broadcast_worker 負責組裝全局狀態並推播，
            確保 monitor 永遠看到所有電梯的最新狀態
        """
        try:
            while True:
                await asyncio.sleep(0.05)   # 讓出 CPU，避免空轉

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
                    await broadcast_queue.put(1)   # 訊號：有狀態變化
                    await asyncio.sleep(DOOR_OPEN_SECONDS)
                    if next_dest is None:
                        continue

                if next_dest is not None:
                    await asyncio.sleep(MOVE_SECONDS)
                    async with self.lock:
                        self._step_floor()
                    logging.info(self.display_floor())
                    await broadcast_queue.put(1)   # 訊號：有狀態變化

        except asyncio.CancelledError:
            logging.debug(f"[{self.elevator_id.upper()}] 生命週期任務已安全取消。")
            raise
