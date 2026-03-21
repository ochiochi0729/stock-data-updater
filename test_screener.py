import sys
import pandas as pd
from core import Logger, load_tickers_from_csv, fetch_bigquery_data, BENCHMARK_TICKER, EVAL_DAYS, STOP_LOSS_THRESHOLD
from strategies.indicators import IndicatorCalculator
from strategies.perfect_order import PerfectOrderScreener
from strategies.cup_with_handle import CupWithHandleScreener
from strategies.breakout import BreakoutScreener

# 過去検証したい日付をここで指定します
TARGET_DATE = '2025-06-03' 

sys.stdout = Logger("test_report.txt")

def run_backtest_logic(screener_class, strategy_name, target_tickers, dict_dfs, actual_eval_date):
    print(f"\n{'='*80}\n▼ 過去検証: {strategy_name}\n{'='*80}")
    screener_class.reset_reasons()
    hit_tickers = []
    aggregate_returns = {d: [] for d in EVAL_DAYS}

    for ticker in target_tickers:
        if ticker not in dict_dfs: continue
        df_full = dict_dfs[ticker]
        try: df = df_full.loc[:TARGET_DATE].copy()
        except KeyError: continue
        if len(df) == 0: continue

        if screener_class.check_conditions(df):
            hit_tickers.append(ticker)
            hit_price = df.iloc[-1]['Close']
            current_idx, max_idx = len(df) - 1, len(df_full) - 1
            
            entry_price = df_full.iloc[current_idx + 1]['Open'] if current_idx + 1 <= max_idx else None
            entry_str = f"{entry_price:,.1f}円" if entry_price else "データなし"
            
            row_1, row_2, is_stopped_out = [], [], False
            for idx, d in enumerate(EVAL_DAYS):
                target_idx = current_idx + d
                if is_stopped_out: perf_str = "損切除外"
                elif target_idx <= max_idx and entry_price:
                    future_price = df_full.iloc[target_idx]['Close']
                    perf = (future_price / entry_price) - 1
                    if perf <= STOP_LOSS_THRESHOLD:
                        is_stopped_out = True
                        perf_str = f"{future_price:,.0f}円({perf:>+5.1%} 損切)"
                    else:
                        aggregate_returns[d].append(perf)
                        perf_str = f"{future_price:,.0f}円({perf:>+5.1%})"
                else: perf_str = "-"
                
                (row_1 if idx < 5 else row_2).append(f"{d:>2}日後: {perf_str}")

            print(f"\n★ 抽出: {ticker:<6s} | 基準日終値: {hit_price:,.1f}円 -> 翌日始値: {entry_str}")
            print(f"   ├ " + " | ".join(row_1))
            print(f"   └ " + " | ".join(row_2))
            print("-" * 80)

    print(f"\n{strategy_name} 抽出完了: {len(hit_tickers)}銘柄")
    if hit_tickers:
        print(f"\n [抽出銘柄 平均]")
        avg_1, avg_2 = [], []
        for idx, d in enumerate(EVAL_DAYS):
            avg_str = f"{d:>2}d: {sum(aggregate_returns[d])/len(aggregate_returns[d]):>+5.1%} ({len(aggregate_returns[d])}銘柄)" if aggregate_returns[d] else f"{d:>2}d: データなし"
            (avg_1 if idx < 5 else avg_2).append(avg_str)
        print(f" ├ " + " | ".join(avg_1) + f"\n └ " + " | ".join(avg_2))

if __name__ == "__main__":
    target_tickers = load_tickers_from_csv()
    if BENCHMARK_TICKER in target_tickers: target_tickers.remove(BENCHMARK_TICKER)
    
    df_all = fetch_bigquery_data(target_date=TARGET_DATE)
    dict_dfs = {ticker: IndicatorCalculator.add_indicators(group.set_index('Date').sort_index()) for ticker, group in df_all.groupby('Ticker')}
    
    actual_eval_date = next(iter(dict_dfs.values())).loc[:TARGET_DATE].index[-1] if len(dict_dfs) > 0 else None

    print(f"\n基準日【{TARGET_DATE}】での過去検証を開始します...\n")
    run_backtest_logic(PerfectOrderScreener, "①パーフェクトオーダー押し目買い", target_tickers, dict_dfs, actual_eval_date)
    run_backtest_logic(CupWithHandleScreener, "②カップ・ウィズ・ハンドル", target_tickers, dict_dfs, actual_eval_date)
    run_backtest_logic(BreakoutScreener, "③底練りからのブレイクアウト", target_tickers, dict_dfs, actual_eval_date)
