# Smart Elevator Dispatch System

## Project Overview

本專案實作一個基於 Python AsyncIO 的智慧電梯調度系統（Smart Elevator Dispatch System）。

系統模擬兩台電梯於十層樓建築中運作，透過 LOOK Scheduling 與成本函數（Cost Function）進行電梯指派，並支援：

- 多乘客同時呼叫電梯
- 即時監控面板
- 非同步並發處理（Concurrent Processing）
- 智慧派梯機制
- 單元測試與整合測試

---

## System Architecture

```text
+----------------+
| Passenger CLI  |
+--------+-------+
         |
         v
+----------------+
| ElevatorServer |
+--------+-------+
         |
         v
+--------------------+
| ElevatorDispatcher |
+--------+-----------+
         |
    +----+----+
    |         |
    v         v
+------+   +------+
| E1   |   | E2   |
+------+   +------+
    |
    v
Broadcast Queue
    |
    v
+----------------+
| Security Monitor|
+----------------+
```

## Core Components

### Elevator
- 維護 current_floor、direction、targets
- 實作 LOOK Scheduling
- 使用 asyncio.Lock 保護共享狀態

### Elevator Dispatcher
- 根據成本函數選擇最適合的電梯
- 支援順路折扣與繞路懲罰

### Elevator Server
- 接收乘客請求
- 指派電梯
- 廣播系統狀態

### Passenger Client
- 提供 CLI 呼叫介面

### Security Monitor
- 即時顯示所有電梯運行狀態

---

## Concurrent Design

本系統採用 AsyncIO 實作並發處理。

同時執行：

- Elevator Lifecycle Tasks
- TCP Server
- Broadcast Worker
- Monitor Connections

相較於多執行緒，可降低 Context Switching 與同步成本。

---

## LOOK Scheduling Algorithm

範例：

```text
目前位置：5F
方向：UP

需求：
7F
9F
2F
```

執行順序：

```text
5 → 7 → 9 → 2
```

降低不必要的來回移動。

---

## Decoupled Broadcasting

電梯不直接進行 Socket I/O。

```python
broadcast_queue.put(...)
```

由 Broadcast Worker 統一處理推播。

優點：

- 降低耦合
- 避免網路延遲影響電梯運行
- 提高系統穩定性

---

## Configuration

config.py

可調整：

- TOTAL_FLOORS
- MOVE_SECONDS
- DOOR_OPEN_SECONDS
- COST_DOOR_PENALTY
- COST_DETOUR_PENALTY
- COST_ON_WAY_DISCOUNT

---

## Running the Project

### Start Server

```bash
python main.py server
```

### Start Monitor

```bash
python main.py monitor
```

### Start Passenger Panel

```bash
python main.py passenger
```

### Demo Mode

```bash
python main.py demo
```

### Run Tests

```bash
python main.py test
```

---

## Technologies Used

- Python 3.9+
- AsyncIO
- TCP Socket
- JSON Protocol
- Unit Testing

---

## Learning Outcomes

- Concurrent Programming
- AsyncIO Event Loop
- TCP Client/Server Design
- LOOK Scheduling
- Producer–Consumer Pattern
- Queue-based Decoupling
- Race Condition Prevention
