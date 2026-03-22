import sys
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from core import Logger, load_tickers_from_csv, fetch_bigquery_data
from strategies.perfect_order import PerfectOrderScreener

sys.stdout = Logger("walk_forward_report.txt")

INITIAL_CAPITAL = 10_000_000
POSITION_LOT = 100
TAKE_PROFIT_PCT = 1.20
STOP_LOSS_PCT = 0.97
START_DATE = '2025-01-01'
END_DATE = '2025-12-31'
BENCHMARK_TICKER = '1306.T'

def run_simulation():
    print(f"{'='*60}\n▼ ウォークフォワード・テスト（時系列シミュレーション）\n{'='*60}")
    print(f"期間: {START_DATE} 〜 {END_DATE}\n初期資金: {INITIAL_CAPITAL:,} 円\n")

    tickers = load_tickers_from_csv()
    if BENCHMARK_TICKER not in tickers: tickers.append(BENCHMARK_TICKER)
    
    print("1. データ取得中...")
    df_all = fetch_bigquery_data(target_date=END_DATE, lookback_days=500)
    dict_dfs = {t: g.set_index('Date').sort_index() for t, g in df_all.groupby('Ticker') if t in tickers}
    
    benchmark_df = dict_dfs[BENCHMARK_TICKER].loc[START_DATE:END_DATE]
    trading_days = benchmark_df.index.tolist()
    
    screener = PerfectOrderScreener()
    print("1.5 シグナル事前計算中...")
    for t, df in dict_dfs.items():
        if t != BENCHMARK_TICKER:
            dict_dfs[t]['is_buy_signal'] = screener.get_all_signals(df)

    cash = INITIAL_CAPITAL
    positions = {} 
    trade_history = []
    daily_equity = []
    candidates_for_tomorrow = []

    print(f"\n2. シミュレーション開始 (全 {len(trading_days)} 営業日)")

    for i, current_date in enumerate(trading_days):
        today_str = current_date.strftime('%Y-%m-%d')

        # --- 売却判定（詳細報告付き） ---
        sold_tickers = []
        for ticker, pos in positions.items():
            df = dict_dfs[ticker]
            if current_date not in df.index: continue
            today_data = df.loc[current_date]
            
            sma25 = today_data.get('SMA25', today_data['Close'])
            entry_price = pos['entry_price']
            tp_price = entry_price * TAKE_PROFIT_PCT
            sl_price = sma25 * STOP_LOSS_PCT
            
            sell_price, reason = None, ""
            if today_data['Open'] <= sl_price: sell_price, reason = today_data['Open'], "損切り(窓開け)"
            elif today_data['Open'] >= tp_price: sell_price, reason = today_data['Open'], "利食い(窓開け)"
            elif today_data['Low'] <= sl_price: sell_price, reason = sl_price, "損切り(日中)"
            elif today_data['High'] >= tp_price: sell_price, reason = tp_price, "利食い(日中)"

            if sell_price:
                profit = (sell_price - entry_price) * pos['shares']
                cash += sell_price * pos['shares']
                trade_history.append({'ticker': ticker, 'entry_date': pos['entry_date'], 'exit_date': today_str, 'profit': profit, 'reason': reason})
                sold_tickers.append(ticker)
                print(f"   💰 [売却] {today_str} : {ticker} {sell_price:,.1f}円 ({reason}) 損益:{profit:>+,.0f}円")

        for t in sold_tickers: del positions[t]

        # --- 購入判定 ---
        for ticker in candidates_for_tomorrow:
            if ticker in positions: continue
            df = dict_dfs[ticker]
            if current_date not in df.index: continue
            prev_close = df.iloc[df.index.get_loc(current_date)-1]['Close']
            today_data = df.loc[current_date]
            
            buy_price = None
            if today_data['Open'] > prev_close: buy_price = today_data['Open']
            elif today_data['High'] > prev_close: buy_price = prev_close
            
            if buy_price:
                cost = buy_price * POSITION_LOT
                if cash >= cost:
                    cash -= cost
                    positions[ticker] = {'entry_price': buy_price, 'shares': POSITION_LOT, 'entry_date': today_str}
                    print(f"   🛒 [購入] {today_str} : {ticker} {buy_price:,.1f}円")

        # --- 明日のための抽出 ---
        candidates_for_tomorrow = [t for t, df in dict_dfs.items() if t != BENCHMARK_TICKER and current_date in df.index and df.at[current_date, 'is_buy_signal']]

        # --- 資産記録 ---
        current_equity = cash + sum(dict_dfs[t].loc[current_date, 'Close'] * p['shares'] for t, p in positions.items() if current_date in dict_dfs[t].index)
        daily_equity.append({'date': current_date, 'equity': current_equity})

    # --- フェーズ3：結果出力（完全復元） ---
    print("\n3. シミュレーション完了！結果を計算します...\n")
    print(f"{'='*60}\n★★★ バックテスト評価レポート ★★★\n{'='*60}")
    
    equity_df = pd.DataFrame(daily_equity).set_index('date')
    final_equity = equity_df['equity'].iloc[-1]
    total_return = (final_equity / INITIAL_CAPITAL) - 1
    
    equity_df['peak'] = equity_df['equity'].cummax()
    mdd = ((equity_df['equity'] - equity_df['peak']) / equity_df['peak']).min()
    
    wins = [t for t in trade_history if t['profit'] > 0]
    losses = [t for t in trade_history if t['profit'] <= 0]
    win_rate = len(wins) / len(trade_history) if trade_history else 0
    pf = sum(t['profit'] for t in wins) / abs(sum(t['profit'] for t in losses)) if losses else float('inf')
    
    bench_return = (benchmark_df['Close'].iloc[-1] / benchmark_df['Close'].iloc[0]) - 1
    
    print(f"【総合パフォーマンス】")
    print(f" ├ 最終総資産  : {final_equity:,.0f} 円")
    print(f" ├ 純利益      : {final_equity - INITIAL_CAPITAL:>+,.0f} 円 ({total_return:>+5.1%})")
    print(f" ├ 最大DD(MDD) : {mdd:>+5.1%}")
    print(f" └ TOPIX比較   : {bench_return:>+5.1%}")
    
    print(f"\n【トレード詳細】")
    print(f" ├ 勝率        : {win_rate:>5.1%} ({len(wins)}勝 / {len(losses)}敗)")
    print(f" └ PF          : {pf:.2f}")

    # グラフ生成
    equity_df['benchmark'] = (benchmark_df['Close'] / benchmark_df['Close'].iloc[0]) * INITIAL_CAPITAL
    plt.figure(figsize=(10, 5))
    plt.plot(equity_df['equity'], label='Strategy')
    plt.plot(equity_df['benchmark'], label='TOPIX', color='gray', linestyle='--')
    plt.legend(); plt.savefig("equity_curve.png")

if __name__ == "__main__":
    run_simulation()
