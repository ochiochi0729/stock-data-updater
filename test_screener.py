import sys
import pandas as pd
from core import Logger, load_tickers_from_csv, fetch_bigquery_data, BENCHMARK_TICKER, EVAL_DAYS, STOP_LOSS_THRESHOLD
from strategies.indicators import IndicatorCalculator
from strategies.perfect_order import PerfectOrderScreener
from strategies.cup_with_handle import CupWithHandleScreener
from strategies.breakout import BreakoutScreener

# ==========================================
# ★ テストしたい基準日を自由に指定（最大5つでも10個でもOK）
# ==========================================
TARGET_DATES = [
    '2025-06-03',
    '2025-07-01',
    '2024-08-01',
    '2025-09-01',
    '2025-10-01'
]

sys.stdout = Logger("test_report.txt")

def run_backtest_logic(screener_class, strategy_name, target_tickers, dict_dfs, target_date):
    print(f"\n{'='*80}\n▼ 過去検証: {strategy_name} (基準日: {target_date})\n{'='*80}")
    screener_class.reset_reasons()
    hit_tickers = []
    aggregate_returns = {d: [] for d in EVAL_DAYS}

    for ticker in target_tickers:
        if ticker not in dict_dfs: continue
        df_full = dict_dfs[ticker]
        
        # ★ 指定した基準日までのデータに絞り込む（未来のデータをカンニングしないため）
        try: df = df_full.loc[:target_date].copy()
        except KeyError: continue
        if len(df) == 0: continue

        if screener_class.check_conditions(df):
            hit_tickers.append(ticker)
            hit_price = df.iloc[-1]['Close']
            
            # 未来のデータ（答え合わせ用）を取得するためのインデックス計算
            # df_full の中で、判定時の最終日が何行目かを正確に特定する
            current_idx = df_full.index.get_loc(df.index[-1])
            max_idx = len(df_full) - 1
            
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
        
    return aggregate_returns

if __name__ == "__main__":
    if not TARGET_DATES:
        print("TARGET_DATES が設定されていません。")
        sys.exit()

    target_tickers = load_tickers_from_csv()
    if BENCHMARK_TICKER in target_tickers: target_tickers.remove(BENCHMARK_TICKER)
    
    # ==========================================
    # ★ 複数日付をカバーするために必要な期間を自動計算
    # ==========================================
    dates = pd.to_datetime(TARGET_DATES)
    min_date = dates.min()
    max_date = dates.max()
    days_diff = (max_date - min_date).days
    
    # 一番古い日付から600日遡り、一番新しい日付から45日後までを一括ダウンロード！
    print("BigQueryから検証に必要な全期間のデータを一括ダウンロード中...")
    df_all = fetch_bigquery_data(target_date=min_date.strftime('%Y-%m-%d'), lookback_days=600, forward_days=45 + days_diff)
    
    # 指標は「全期間」について1回だけ計算する（超高速）
    dict_dfs = {ticker: IndicatorCalculator.add_indicators(group.set_index('Date').sort_index()) for ticker, group in df_all.groupby('Ticker')}
    
    # 各戦略ごとの全期間総合リターンを保存する箱
    grand_results = {
        "①パーフェクトオーダー": {d: [] for d in EVAL_DAYS},
        "②カップ・ウィズ・ハンドル": {d: [] for d in EVAL_DAYS},
        "③ブレイクアウト": {d: [] for d in EVAL_DAYS}
    }

    print(f"\n指定された {len(TARGET_DATES)} つの基準日でバックテストを開始します...\n")

    # 指定された日付ごとにループしてテスト実行
    for t_date in TARGET_DATES:
        print(f"\n\n{'#'*80}\n# 基準日: {t_date} のスクリーニング\n{'#'*80}")
        
        res1 = run_backtest_logic(PerfectOrderScreener, "①パーフェクトオーダー押し目買い", target_tickers, dict_dfs, t_date)
        res2 = run_backtest_logic(CupWithHandleScreener, "②カップ・ウィズ・ハンドル", target_tickers, dict_dfs, t_date)
        res3 = run_backtest_logic(BreakoutScreener, "③底練りからのブレイクアウト", target_tickers, dict_dfs, t_date)
        
        # 今回の抽出結果を、全体の箱（grand_results）に合流させる
        for d in EVAL_DAYS:
            grand_results["①パーフェクトオーダー"][d].extend(res1[d])
            grand_results["②カップ・ウィズ・ハンドル"][d].extend(res2[d])
            grand_results["③ブレイクアウト"][d].extend(res3[d])

    # ==========================================
    # ★ 最後に各戦略の「全基準日での総合平均結果」を出力する
    # ==========================================
    print(f"\n\n{'='*80}")
    print("★★★ 全基準日の総合平均パフォーマンス ★★★")
    print(f"{'='*80}")
    
    for strategy_name, agg_returns in grand_results.items():
        print(f"\n▼ {strategy_name} (総合)")
        avg_1, avg_2 = [], []
        
        # どの期間でも1度も抽出されなかった戦略のスキップ処理
        total_trades = max([len(agg_returns[d]) for d in EVAL_DAYS] + [0])
        if total_trades == 0:
            print("  全期間を通じて抽出銘柄なし")
            continue
            
        for idx, d in enumerate(EVAL_DAYS):
            survivors = len(agg_returns[d])
            if survivors > 0:
                avg_perf = sum(agg_returns[d]) / survivors
                avg_str = f"{d:>2}d: {avg_perf:>+5.1%} ({survivors}銘柄)"
            else:
                avg_str = f"{d:>2}d: データなし"
            (avg_1 if idx < 5 else avg_2).append(avg_str)
            
        print(f" ├ " + " | ".join(avg_1))
        print(f" └ " + " | ".join(avg_2))
