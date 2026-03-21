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
TARGET_ROE = 17                # ROE 17%以上
TARGET_GROWTH = 25             # 成長率 25%以上
MIN_MARKET_CAP = 10000000000   # 時価総額 100億円以上
MIN_VOLUME = 100000            # 30日平均出来高 10万株以上

def fetch_tradingview_canslim_stocks():
    print(f"{'='*60}\n▼ TradingViewスクリーナー（CANSLIM 厳選版）実行中...\n{'='*60}")
    
    payload = {
        "filter": [
            # 基礎条件
            {"left": "type", "operation": "equal", "right": "stock"},
            {"left": "subtype", "operation": "equal", "right": "common"},
            {"left": "is_primary", "operation": "equal", "right": True},
            
            # 業績条件（R, A, C）
            {"left": "return_on_equity", "operation": "egreater", "right": TARGET_ROE},
            {"left": "earnings_per_share_diluted_yoy_growth_ttm", "operation": "egreater", "right": TARGET_GROWTH},
            {"left": "total_revenue_yoy_growth_ttm", "operation": "egreater", "right": TARGET_GROWTH},
            
            # ★ 追加：トレンド条件（株価が200日線、50日線より上にある）
            {"left": "close", "operation": "egreater", "right": "SMA200"},
            {"left": "close", "operation": "egreater", "right": "SMA50"},
            
            # ★ 追加：流動性・規模条件（時価総額100億円以上、平均出来高10万株以上）
            {"left": "market_cap_basic", "operation": "egreater", "right": MIN_MARKET_CAP},
            {"left": "average_volume_30d_calc", "operation": "egreater", "right": MIN_VOLUME}
        ],
        "options": {"lang": "ja"},
        "markets": ["japan"],
        "symbols": {"query": {"types": []}, "tickers": []},
        
        "columns": [
            "name",                                      # 0: ティッカー
            "description",                               # 1: 会社名
            "return_on_equity",                          # 2: ROE
            "total_revenue_yoy_growth_ttm",              # 3: 売上成長率
            "earnings_per_share_diluted_yoy_growth_ttm", # 4: EPS成長率
            "close",                                     # 5: 現在値
            "market_cap_basic",                          # 6: 時価総額
            "average_volume_30d_calc"                    # 7: 30日平均出来高
        ],
        "sort": {"sortBy": "market_cap_basic", "sortOrder": "desc"},
        "range": [0, 150] 
    }

    try:
        response = requests.post(SCANNER_URL, json=payload, timeout=15)
        
        if response.status_code != 200:
            print(f"❌ TradingViewに弾かれました (Error {response.status_code})")
            print(f"【エラー詳細】: {response.text}")
            return []
            
        data = response.json()
        
        if "data" not in data or not data["data"]:
            print("❌ 条件に合致する銘柄は見つかりませんでした。（条件が厳しすぎる可能性があります）")
            return []
            
        print(f"✅ 業績・トレンド・流動性の全てを満たす最強株が {len(data['data'])} 銘柄見つかりました！\n")
        
        hit_results = []
        for item in data["data"]:
            d = item["d"]
            ticker = f"{d[0]}.T"
            
            res = {
                "Ticker": ticker,
                "Name": d[1],
                "ROE": d[2],
                "Rev_Growth": d[3],
                "EPS_Growth": d[4],
                "Close": d[5],
                "MarketCap": d[6],
                "Volume30d": d[7]
            }
            hit_results.append(res)
            
        return hit_results

    except Exception as e:
        print(f"❌ 通信エラー: {e}")
        return []

if __name__ == "__main__":
    hit_stocks = fetch_tradingview_canslim_stocks()
    
    if hit_stocks:
        print(f"{'='*60}\n★★★ CANSLIM 厳選・最強銘柄リスト ★★★\n{'='*60}")
        for r in hit_stocks:
            print(f"\n★ {r['Ticker']} : {r['Name']}")
            
            # データが空の場合の安全処理
            roe_val = r['ROE'] if r['ROE'] else 0
            rev_val = r['Rev_Growth'] if r['Rev_Growth'] else 0
            eps_val = r['EPS_Growth'] if r['EPS_Growth'] else 0
            mcap_val = r['MarketCap'] if r['MarketCap'] else 0
            vol_val = r['Volume30d'] if r['Volume30d'] else 0
            close_val = r['Close'] if r['Close'] else 0
            
            # 単位変換（見やすくする）
            mcap_oku = mcap_val / 100000000  # 億円
            vol_man = vol_val / 10000        # 万株
            
            print(f" ├ 現在値    : {close_val:,.1f} 円")
            print(f" ├ ROE       : {roe_val:.1f}%")
            print(f" ├ 成長率    : 売上 {rev_val:.1f}% | EPS {eps_val:.1f}% (TTM)")
            print(f" ├ 規模・流動: 時価総額 {mcap_oku:,.0f}億円 | 平均出来高 {vol_man:,.1f}万株")
            print(f" └ チャート  : https://finance.yahoo.co.jp/quote/{r['Ticker']}")
