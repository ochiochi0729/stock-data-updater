import sys
import requests
import pandas as pd
from core import Logger

sys.stdout = Logger("canslim_report.txt")

# ==========================================
# ★ TradingViewスクリーナー（日本株）の設定
# ==========================================
SCANNER_URL = "https://scanner.tradingview.com/japan/scan"

# ★ 判定基準の設定
TARGET_ROE = 17          # ROE 17%以上
TARGET_GROWTH = 25       # 成長率 25%以上

def fetch_tradingview_canslim_stocks():
    print(f"{'='*60}\n▼ TradingViewスクリーナー（CANSLIM法）実行中...\n{'='*60}")
    
    # ★ 最新のTradingView API仕様に合わせた項目名
    payload = {
        "filter": [
            {"left": "type", "operation": "equal", "right": "stock"},
            {"left": "subtype", "operation": "equal", "right": "common"},
            {"left": "is_primary", "operation": "equal", "right": True}, # 代表銘柄に絞る
            
            # 1. R (ROE 17%以上)
            {"left": "return_on_equity", "operation": "egreater", "right": TARGET_ROE},
            
            # 2. A & C (EPS成長率 TTM 25%以上)
            {"left": "earnings_per_share_diluted_yoy_growth_ttm", "operation": "egreater", "right": TARGET_GROWTH},
            
            # 3. A & C (売上成長率 TTM 25%以上)
            {"left": "total_revenue_yoy_growth_ttm", "operation": "egreater", "right": TARGET_GROWTH}
        ],
        "options": {"lang": "ja"},
        "markets": ["japan"],
        "symbols": {"query": {"types": []}, "tickers": []},
        
        # 取得したいデータ列（列の名前も完全に一致させる必要があります）
        "columns": [
            "name",                                      # 0: ティッカー
            "description",                               # 1: 会社名
            "return_on_equity",                          # 2: ROE
            "total_revenue_yoy_growth_ttm",              # 3: 売上成長率
            "earnings_per_share_diluted_yoy_growth_ttm", # 4: EPS成長率
            "total_shares_outstanding"                   # 5: 発行済株式数
        ],
        "sort": {"sortBy": "market_cap_basic", "sortOrder": "desc"},
        "range": [0, 150] 
    }

    try:
        response = requests.post(SCANNER_URL, json=payload, timeout=15)
        
        # ★ 安全装置：エラーの理由を詳細に表示する
        if response.status_code != 200:
            print(f"❌ TradingViewに弾かれました (Error {response.status_code})")
            print(f"【エラー詳細】: {response.text}")
            print("※ 項目名（columns または filter）の仕様が変更された可能性があります。")
            return []
            
        data = response.json()
        
        if "data" not in data or not data["data"]:
            print("❌ 条件に合致する銘柄は見つかりませんでした。")
            return []
            
        print(f"✅ 相場最強の成長株が {len(data['data'])} 銘柄見つかりました！\n")
        
        hit_results = []
        for item in data["data"]:
            d = item["d"]
            # 日本株フォーマット（例: 7203.T）に変換
            ticker = f"{d[0]}.T"
            
            res = {
                "Ticker": ticker,
                "Name": d[1],
                "ROE": d[2],
                "Rev_Growth": d[3],
                "EPS_Growth": d[4],
                "Shares": d[5]
            }
            hit_results.append(res)
            
        return hit_results

    except Exception as e:
        print(f"❌ 通信エラー: {e}")
        return []

if __name__ == "__main__":
    hit_stocks = fetch_tradingview_canslim_stocks()
    
    if hit_stocks:
        print(f"{'='*60}\n★★★ CANSLIM（財務最強）銘柄リスト ★★★\n{'='*60}")
        for r in hit_stocks:
            print(f"\n★ {r['Ticker']} : {r['Name']}")
            
            # データが空の場合の安全処理
            roe_val = r['ROE'] if r['ROE'] else 0
            rev_val = r['Rev_Growth'] if r['Rev_Growth'] else 0
            eps_val = r['EPS_Growth'] if r['EPS_Growth'] else 0
            shares_val = r['Shares'] if r['Shares'] else 0
            
            print(f" ├ ROE       : {roe_val:.1f}%")
            print(f" ├ 成長率(TTM): 売上 {rev_val:.1f}% | EPS {eps_val:.1f}%")
            print(f" └ 発行済株式: {shares_val:,.0f} 株")
            print(f" └ チャート確認: https://finance.yahoo.co.jp/quote/{r['Ticker']}")
