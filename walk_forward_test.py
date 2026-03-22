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
    print(f"{'='*60}\n▼ 爆速ウォークフォワード・テスト\n{'='*60}")
    
    tickers = load_tickers_from_csv()
    if BENCHMARK_TICKER not in tickers: tickers.append(BENCHMARK_TICKER)
    
    print("1. BigQueryからデータ取得中...")
    df_all = fetch_bigquery_data(target_date=END_DATE, lookback_days=500, forward_days=0)
    dict_dfs = {t: g.set_index('Date').sort_index() for t, g in df_all.groupby('Ticker') if t in tickers}

    benchmark_df = dict_dfs[BENCHMARK_TICKER].loc[START_DATE:END_DATE]
    trading_days = benchmark_df.index.tolist()
    
    screener = PerfectOrderScreener()

    # ★ 高速化の鍵：全銘柄のシグナルをループ前に一括計算！
    print("1.5 全銘柄のシグナルを事前計算中...")
    for t, df in dict_dfs.items():
        if t != BENCHMARK_TICKER:
            dict_dfs[t]['is_buy_signal'] = screener.get_all_signals(df)

    cash = INITIAL_CAPITAL
    positions = {} 
    trade_history = []
    daily_equity = []
    candidates_for_tomorrow = []

    print(f"\n2. シミュレーション開始 (全 {len(trading_days)} 日)")

    for i, current_date in enumerate(trading_days):
        today_str = current_date.strftime('%Y-%m-%d')

        # --- 既存ポジションの売却判定と「報告」 ---
        sold_tickers = []
        for ticker, pos in positions.items():
            df = dict_dfs[ticker]
            if current_date not in df.index: continue
            td = df.loc[current_date]
            
            # (売却ロジック...利食い・損切り判定)
            sl_price = td.get('SMA25', td['Close']) * STOP_LOSS_PCT
            tp_price = pos['entry_price'] * TAKE_PROFIT_PCT
            
            sell_price, reason = None, ""
            if td['Open'] <= sl_price: sell_price, reason = td['Open'], "損切り(窓)"
            elif td['Open'] >= tp_price: sell_price, reason = td['Open'], "利食い(窓)"
            elif td['Low'] <= sl_price: sell_price, reason = sl_price, "損切り"
            elif td['High'] >= tp_price: sell_price, reason = tp_price, "利食い"

            if sell_price:
                profit = (sell_price - pos['entry_price']) * pos['shares']
                cash += sell_price * pos['shares']
                trade_history.append({'ticker': ticker, 'profit': profit, 'reason': reason})
                sold_tickers.append(ticker)
                # ★売却報告の追加
                print(f"   💰 [売却] {today_str} : {ticker} ({reason}) 損益:{profit:>+,.0f}円")

        for t in sold_tickers: del positions[t]

        # --- 購入（計算済みのシグナルを「見るだけ」） ---
        for ticker in candidates_for_tomorrow:
            if ticker in positions: continue
            df = dict_dfs[ticker]
            if current_date not in df.index: continue
            
            # (購入条件判定...始値が前日終値より高いか等)
            # ここは日次審査のロジックを維持
            prev_close = df.iloc[df.index.get_loc(current_date)-1]['Close']
            if df.loc[current_date, 'Open'] > prev_close:
                buy_price = df.loc[current_date, 'Open']
                cost = buy_price * POSITION_LOT
                if cash >= cost:
                    cash -= cost
                    positions[ticker] = {'entry_price': buy_price, 'shares': POSITION_LOT, 'entry_date': today_str}
                    print(f"   🛒 [購入] {today_str} : {ticker} {buy_price:,.1f}円")

        # --- 明日のための抽出（爆速！） ---
        candidates_for_tomorrow = [t for t, df in dict_dfs.items() 
                                  if t != BENCHMARK_TICKER and current_date in df.index 
                                  and df.at[current_date, 'is_buy_signal']]

        # --- 資産記録 ---
        current_eq = cash + sum(dict_dfs[t].loc[current_date, 'Close'] * p['shares'] for t, p in positions.items() if current_date in dict_dfs[t].index)
        daily_equity.append({'date': current_date, 'equity': current_eq})

    # --- 評価とグラフ ---
    equity_df = pd.DataFrame(daily_equity).set_index('date')
    bench_initial = benchmark_df['Close'].iloc[0]
    equity_df['benchmark'] = (benchmark_df['Close'] / bench_initial) * INITIAL_CAPITAL
    
    plt.figure(figsize=(10, 5))
    plt.plot(equity_df['equity'], label='Strategy')
    plt.plot(equity_df['benchmark'], label='TOPIX', color='gray', linestyle='--')
    plt.legend()
    plt.savefig("equity_curve.png")
    print(f"\n📈 資産推移を 'equity_curve.png' に保存しました。")

if __name__ == "__main__":
    run_simulation()
