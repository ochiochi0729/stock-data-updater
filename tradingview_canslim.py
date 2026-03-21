import sys
import time
import pandas as pd
import yfinance as yf
from core import Logger, load_tickers_from_csv

sys.stdout = Logger("canslim_report.txt")

# ★ 判定基準
TARGET_ROE = 0.17        # ROE 17%以上
TARGET_GROWTH = 0.25     # 売上・利益の成長率 25%以上

def analyze_fundamentals(ticker):
    print(f"[{ticker}] 財務データを取得中...", end="", flush=True)
    t = yf.Ticker(ticker)
    info = t.info
    
    # --- 結果を格納する箱 ---
    res = {
        "Ticker": ticker,
        "Name": info.get("longName", ticker),
        "ROE": None,
        "Rev_Growth": None,
        "Net_Inc_Growth": None,
        "Shares": info.get("sharesOutstanding", "不明"),
        "Institutions": "データなし",
        "Pass": False,
        "Drop_Reason": ""
    }
    
    # 1. ROEの判定
    roe = info.get("returnOnEquity")
    if roe is not None:
        res["ROE"] = roe
        if roe < TARGET_ROE:
            res["Drop_Reason"] = f"ROE不足 ({roe:.1%})"
            print(f" ❌ {res['Drop_Reason']}")
            return res
    else:
        res["Drop_Reason"] = "ROEデータなし"
        print(" ❌ ROEデータなし")
        return res

    # 2. 年次財務データ（Income Statement）の取得
    try:
        financials = t.income_stmt
        if financials is None or financials.empty or financials.shape[1] < 2:
            res["Drop_Reason"] = "過去2年分の財務データなし"
            print(" ❌ 財務データ不足")
            return res
            
        # 売上高（Total Revenue）と純利益（Net Income）を取得
        rev_current = financials.loc["Total Revenue"].iloc[0]
        rev_prev = financials.loc["Total Revenue"].iloc[1]
        inc_current = financials.loc["Net Income"].iloc[0]
        inc_prev = financials.loc["Net Income"].iloc[1]
        
        # 成長率の計算
        if rev_prev and rev_prev > 0:
            res["Rev_Growth"] = (rev_current / rev_prev) - 1
        if inc_prev and inc_prev > 0:
            res["Net_Inc_Growth"] = (inc_current / inc_prev) - 1
            
        # 成長率の判定
        if res["Rev_Growth"] is None or res["Rev_Growth"] < TARGET_GROWTH:
            res["Drop_Reason"] = f"売上成長率不足 ({res['Rev_Growth']:.1%} if res['Rev_Growth'] is not None else '計算不可')"
            print(f" ❌ {res['Drop_Reason']}")
            return res
            
        if res["Net_Inc_Growth"] is None or res["Net_Inc_Growth"] < TARGET_GROWTH:
            res["Drop_Reason"] = f"純利益成長率不足 ({res['Net_Inc_Growth']:.1%} if res['Net_Inc_Growth'] is not None else '計算不可')"
            print(f" ❌ {res['Drop_Reason']}")
            return res
            
    except Exception as e:
        res["Drop_Reason"] = f"財務データ解析エラー"
        print(" ❌ 財務データ解析エラー")
        return res

    # 3. 参考情報：機関投資家の取得
    try:
        inst = t.institutional_holders
        if inst is not None and not inst.empty:
            top_holders = inst['Holder'].head(3).tolist()
            res["Institutions"] = ", ".join(top_holders)
    except:
        pass

    # すべての条件をクリア！
    res["Pass"] = True
    print(" ✅ 条件クリア！")
    return res

if __name__ == "__main__":
    print(f"{'='*60}\n▼ ファンダメンタルズ（CANSLIMプロトタイプ）スクリーニング\n{'='*60}")
    
    # ★ 激重処理になるため、まずは日本の代表的な銘柄10個だけでテスト稼働します
    test_tickers = ["7203.T", "6920.T", "9984.T", "6861.T", "6098.T", "4385.T", "8035.T", "9983.T", "7974.T", "6758.T"]
    
    print(f"対象銘柄数: {len(test_tickers)}銘柄（テスト稼働）\n")
    
    hit_results = []
    
    for ticker in test_tickers:
        res = analyze_fundamentals(ticker)
        if res["Pass"]:
            hit_results.append(res)
        time.sleep(1) # Yahooに弾かれないよう1秒待機
        
    print(f"\n{'='*60}\n抽出完了: {len(hit_results)}銘柄\n{'='*60}")
    
    for r in hit_results:
        print(f"\n★ 抽出: {r['Ticker']} ({r['Name']})")
        print(f" ├ ROE       : {r['ROE']:.1%}")
        print(f" ├ 売上成長率: {r['Rev_Growth']:.1%} (YoY)")
        print(f" ├ 利益成長率: {r['Net_Inc_Growth']:.1%} (YoY)")
        print(f" ├ 発行済株式: {r['Shares']:,} 株")
        print(f" └ 機関投資家: {r['Institutions']}")
