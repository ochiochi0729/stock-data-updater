import os
import sys
import pandas as pd
from core import Logger, load_tickers_from_csv, fetch_bigquery_data, BENCHMARK_TICKER
from strategies.indicators import IndicatorCalculator
from strategies.perfect_order import PerfectOrderScreener
from strategies.cup_with_handle import CupWithHandleScreener
from strategies.breakout import BreakoutScreener

sys.stdout = Logger("report.txt")

def run_daily_logic(screener_class, strategy_name, target_tickers, dict_dfs):
    print(f"\n{'='*60}\n▼ {strategy_name}\n{'='*60}")
    screener_class.reset_reasons()
    hit_tickers = []
    
    for ticker in target_tickers:
        if ticker not in dict_dfs or len(dict_dfs[ticker]) == 0: continue
        df = dict_dfs[ticker]
        if screener_class.check_conditions(df):
            hit_tickers.append(ticker)
            
            # ★ YahooファイナンスのURLを生成（スマホからワンクリックで飛べます）
            yfinance_url = f"https://finance.yahoo.co.jp/quote/{ticker}"
            
            print(f"★ 抽出: {ticker:<6s} | 本日終値: {df.iloc[-1]['Close']:,.1f}円")
            print(f"   └ チャート確認: {yfinance_url}")
            
    print(f"\n抽出完了: {len(hit_tickers)}銘柄")
    if hit_tickers: print(f"合致銘柄: {', '.join(hit_tickers)}")
    print("\n[脱落理由]")
    for reason, count in sorted(screener_class.drop_reasons.items()):
        print(f"{reason}: {count} 銘柄")

if __name__ == "__main__":
    target_tickers = load_tickers_from_csv()
    if BENCHMARK_TICKER in target_tickers: target_tickers.remove(BENCHMARK_TICKER)
    
    df_all = fetch_bigquery_data(target_date=None)
    dict_dfs = {ticker: IndicatorCalculator.add_indicators(group.set_index('Date').sort_index()) for ticker, group in df_all.groupby('Ticker')}
        
    print("\n本日のスクリーニングを開始します...\n")
    run_daily_logic(PerfectOrderScreener, "①パーフェクトオーダー押し目買い", target_tickers, dict_dfs)
    run_daily_logic(CupWithHandleScreener, "②カップ・ウィズ・ハンドル", target_tickers, dict_dfs)
    run_daily_logic(BreakoutScreener, "③底練りからのブレイクアウト", target_tickers, dict_dfs)
    print("\nすべてのスクリーニングが完了しました。")
