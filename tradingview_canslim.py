import sys
import json
import requests
import pandas as pd
from core import Logger # 既存のcore.pyからLoggerを拝借

sys.stdout = Logger("canslim_report.txt")

# ==========================================
# ★ TradingViewスクリーナー（日本株）の設定
# ==========================================
SCANNER_URL = "https://scanner.tradingview.com/japan/scan"

# ★ 判定基準の設定
TARGET_ROE = 17          # ROE 17%以上
TARGET_GROWTH = 25       # 売上・EPS成長率 25%以上

def fetch_tradingview_canslim_stocks():
    print(f"{'='*60}\n▼ TradingViewスクリーナー（CANSLIM法）実行中...\n{'='*60}")
    
    # ★ TradingViewへの「手紙（リクエストPayload）」を作成
    # このJSONデータが「裏口」の鍵です。
    payload = {
        "filter": [
            # 日本市場の上場株式に限定
            {"left": "market", "operation": "equal", "right": "japan"},
            {"left": "is_primary", "operation": "equal", "right": True},
            {"left": "type", "operation": "in_range", "right": ["stock"]},
            {"left": "subtype", "operation": "in_range", "right": ["common"]},
            
            # --- 【CANSLIM条件の指定】 ---
            # 1. R (ROE 17%以上)
            {"left": "return_on_equity", "operation": "egreater", "right": TARGET_ROE},
            
            # 2. A (直近4四半期(TTM)の売上・EPS成長率 25%以上)
            {"left": "revenue_growth_ttm", "operation": "egreater", "right": TARGET_GROWTH},
            {"left": "earnings_per_share_diluted_growth_ttm", "operation": "egreater", "right": TARGET_GROWTH},
            
            # 3. C (直近1四半期の売上・EPS成長率 25%以上)
            {"left": "revenue_growth_quarterly", "operation": "egreater", "right": TARGET_GROWTH},
            {"left": "earnings_per_share_diluted_growth_quarterly", "operation": "egreater", "right": TARGET_GROWTH}
        ],
        "options": {"lang": "ja"},
        "markets": ["japan"],
        "symbols": {"query": {"types": []}, "tickers": []},
        
        # --- 【取得したいフィールド（列）の指定】 ---
        "columns": [
            "name",                                         # 0: ティッカー
            "exchange",                                     # 1: 取引所
            "description",                                  # 2: 会社名
            "return_on_equity",                             # 3: ROE
            "revenue_growth_quarterly",                     # 4: 四半期売上成長率
            "earnings_per_share_diluted_growth_quarterly",  # 5: 四半期EPS成長率
            "revenue_growth_ttm",                           # 6: TTM売上成長率
            "earnings_per_share_diluted_growth_ttm",        # 7: TTMEPS成長率
            "total_shares_outstanding",                     # 8: 発行済株式数
            "held_by_institutions"                          # 9: 機関投資家保有比率(%)
        ],
        # 時価総額が大きい順にソート
        "sort": {"sortBy": "market_cap_basic", "sortOrder": "desc"},
        # 取得件数（最大150件。もし足りなければここを増やす）
        "range": [0, 150] 
    }

    try:
        # TradingViewのサーバーにPOSTリクエストを送信
        response = requests.post(SCANNER_URL, json=payload, timeout=15)
        response.raise_for_status() # エラーがあれば例外を投げる
        
        # JSONをパース
        data = response.json()
        
        if "data" not in data or not data["data"]:
            print("❌ 条件に合致する銘柄は見つかりませんでした。")
            return []
            
        print(f"✅ 相場最強の成長株が {len(data['data'])} 銘柄見つかりました！\n")
        
        hit_results = []
        for item in data["data"]:
            d = item["d"] #columnsで指定した順に配列に入っている
            
            # ティッカー名を日本株の形式（例: TSE:7203 -> 7203.T）に変換
            ticker_raw = d[0]
            exchange_raw = d[1]
            ticker = ticker_raw
            if exchange_raw == "TSE": # 東証の場合のみ.Tをつける（簡易対応）
                ticker = f"{ticker_raw}.T"
                
            res = {
                "Ticker": ticker,
                "Name": d[2],
                "ROE": d[3],
                "C_Rev": d[4],
                "C_EPS": d[5],
                "A_Rev": d[6],
                "A_EPS": d[7],
                "Shares": d[8],
                "Inst_Hold": d[9]
            }
            hit_results.append(res)
            
        return hit_results

    except Exception as e:
        print(f"❌ TradingViewスクリーナー接続エラー: {e}")
        return []

if __name__ == "__main__":
    hit_stocks = fetch_tradingview_canslim_stocks()
    
    if hit_stocks:
        print(f"{'='*60}\n★★★ CANSLIM（財務最強）銘柄リスト ★★★\n{'='*60}")
        for r in hit_stocks:
            print(f"\n★ {r['Ticker']} : {r['Name']}")
            print(f" ├ ROE       : {r['ROE']:.1f}%")
            print(f" ├ 【C】 直近四半期成長 (YoY)")
            print(f" │ └ 売上: {r['C_Rev']:>+5.1%}% | EPS: {r['C_EPS']:>+5.1%}%")
            print(f" ├ 【A】 年間持続成長 (TTM YoY)")
            print(f" │ └ 売上: {r['A_Rev']:>+5.1%}% | EPS: {r['A_EPS']:>+5.1%}%")
            print(f" ├ 【S】 発行済株式: {r['Shares']:,} 株")
            print(f" └ 【I】 機関投資家: {r['Inst_Hold']:.1f}% 保有")
