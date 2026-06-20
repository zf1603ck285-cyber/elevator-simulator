"""
config.py — 全域可調參數
所有常數集中在這裡，修改行為只需動這個檔案。
"""

TOTAL_FLOORS        = 10    # 大樓總層數
MOVE_SECONDS        = 1.0   # 每層移動耗時（秒）
DOOR_OPEN_SECONDS   = 0.5   # 開關門耗時（秒）

# 調度成本函數參數
COST_DOOR_PENALTY   = 2.0   # 每個排定停靠點的開關門懲罰（秒）
COST_DETOUR_PENALTY = 15.0  # 電梯需反向/已越過時的額外懲罰
COST_ON_WAY_DISCOUNT= 0.8   # 順路接客的成本折扣係數

# 網路設定
SERVER_HOST         = "127.0.0.1"
SERVER_PORT         = 8888
