"""
dispatcher.py — 智慧調度器

職責：
  - 根據 LOOK-aware 成本函數選出最適合的電梯
  - 成本相同時隨機 tie-breaking，避免永遠偏向第一台
"""

import random
from typing import List

from .config import (
    COST_DETOUR_PENALTY,
    COST_DOOR_PENALTY,
    COST_ON_WAY_DISCOUNT,
    MOVE_SECONDS,
)
from .elevator import Elevator


class ElevatorDispatcher:
    """
    LOOK-aware 成本函數調度器。

    成本計算邏輯：
      base_cost  = 距離（秒）+ 停靠點數 × COST_DOOR_PENALTY
      IDLE       → base_cost
      順路接客   → base_cost × COST_ON_WAY_DISCOUNT
      其他       → base_cost + COST_DETOUR_PENALTY
    """

    def __init__(self, elevators: List[Elevator]):
        self.elevators = elevators

    def calculate_cost(self, snapshot: dict, pickup: int, destination: int) -> float:
        curr      = snapshot["current_floor"]
        direction = snapshot["direction"]
        targets   = snapshot["targets"]

        distance    = abs(curr - pickup)
        request_dir = "UP" if destination > pickup else "DOWN"
        base_cost   = distance * MOVE_SECONDS + len(targets) * COST_DOOR_PENALTY

        if direction == "IDLE":
            return base_cost

        if direction == request_dir:
            on_way = (
                (direction == "UP"   and curr <= pickup) or
                (direction == "DOWN" and curr >= pickup)
            )
            if on_way:
                return base_cost * COST_ON_WAY_DISCOUNT

        return base_cost + COST_DETOUR_PENALTY

    async def select_best_elevator(self, pickup: int, destination: int) -> Elevator:
        """
        選成本最低的電梯。
        成本相同時隨機選一台（tie-breaking），避免固定偏向第一台造成負載不均。
        """
        costs = []
        for e in self.elevators:
            snap = await e.get_state_snapshot()
            costs.append((self.calculate_cost(snap, pickup, destination), e))

        min_cost   = min(c for c, _ in costs)
        candidates = [e for c, e in costs if c == min_cost]
        return random.choice(candidates)
