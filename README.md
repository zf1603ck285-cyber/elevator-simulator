# Elevator Simulator

使用 Python `asyncio` 實作的雙電梯模擬系統。

功能包含：

- LOOK 排程演算法
- 多電梯並行運作
- TCP Client / Server 架構
- 即時監控面板
- 單元測試與整合測試

---

## 執行環境

- Python 3.9 以上
- 不需安裝額外套件

---

## 快速開始

### Demo 模式（推薦）

直接展示兩台電梯同時運作：

```bash
python ElevatorSimulator_v3.py demo
```

### 執行測試

```bash
python ElevatorSimulator_v3.py test
```

---

## 系統模式

### 1. Server（控制中心）

```bash
python ElevatorSimulator_v3.py server
```

負責：

- 接收乘客請求
- 電梯調度
- 狀態廣播

### 2. Passenger（乘客端）

```bash
python ElevatorSimulator_v3.py passenger
```

輸入格式：

```text
發送呼叫: 2 9
```

表示：

```text
目前在 2F
要前往 9F
```

離開：

```text
exit
```

### 3. Monitor（監控中心）

```bash
python ElevatorSimulator_v3.py monitor
```

即時顯示：

- 電梯位置
- 移動方向
- 停靠樓層

### 4. Demo（單終端展示）

```bash
python ElevatorSimulator_v3.py demo
```

會自動：

- 啟動 Server
- 啟動 Monitor
- 自動送出乘客請求

適合展示與驗收。

---

## 電梯排程方式

本系統採用 LOOK Scheduling。

原則：

1. 持續朝目前方向移動
2. 順路樓層依序停靠
3. 前方沒有請求後才反向
4. 閒置時優先選擇最近樓層

例如：

```text
目前：5F ↑

請求：
8F
10F
2F
```

執行順序：

```text
5 → 8 → 10 → 2
```

---

## 系統架構

```text
Passenger
    │
    ▼
Server
    │
    ▼
Dispatcher
    │
 ┌──┴──┐
 ▼     ▼
E1     E2
 │      │
 ▼      ▼
Queue  Queue
 │      │
 ▼      ▼
Broadcast Worker
 │
 ▼
Monitor
```

---

## 主要設計特色

### asyncio 並行運作

每台電梯都是獨立 Task，因此：

- 電梯 A 移動時
- 電梯 B 仍可繼續運作
- Server 仍可接收新請求

不會互相阻塞。

### Queue 解耦

電梯只負責寫入 Queue。

真正的 TCP 傳送由 Broadcast Worker 處理。

好處：

- 慢速 Client 不會卡住電梯
- 業務邏輯與網路層分離

### 成本函數調度

調度器會考慮：

- 距離成本
- 停靠成本
- 繞路懲罰
- 順路折扣

選出最適合的電梯。

---

## 測試內容

執行：

```bash
python ElevatorSimulator_v3.py test
```

測試包含：

- 電梯上下移動
- 重複請求去重
- LOOK 方向切換
- 最近樓層選擇
- move() 介面驗證
- TCP Socket 整合測試
- 非法請求驗證

共 12 項測試。

---

## 作者

資料結構與並行程式設計練習專案

- Python asyncio
- TCP Socket
- LOOK Scheduling
- Unit Test / Integration Test
