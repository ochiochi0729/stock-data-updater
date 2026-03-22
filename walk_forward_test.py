import sys
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from core import Logger, load_tickers_from_csv, fetch_bigquery_data
from strategies.perfect_order import PerfectOrderScreener
from strategies.cup_with_handle import CupWithHandleScreener

# ==========================================
# ★ 戦略の選択
# ==========================================
# "PO" = パーフェクトオーダー / "CWH" = カップウィズハンドル
STRATEGY_TYPE = "PO" 

STRATEGY_MAP = {
    "PO": {"class": PerfectOrderScreener, "name": "Perfect Order"},
    "CWH": {"class": CupWithHandleScreener, "name": "Cup with Handle"}
}

sys.stdout = Logger("walk_forward_report.txt")

# シミュレーション設定
INITIAL_CAPITAL = 10_000_000
POSITION_LOT = 100
TAKE_PROFIT_PCT = 1.20
STOP_LOSS_PCT = 0.97
START_DATE = '2025-01-01'
END_DATE = '2025-12-31'
BENCHMARK_TICKER = '1306.T'

def run_simulation():
    selected = STRATEGY_MAP[STRATEGY_TYPE]
    screener = selected["class"]()
    
    print(f"{'='*60}\n▼ ウォークフォワード・テスト\n  戦略: {selected['name']}\n{'='*60}")
    print(f"期間: {START_DATE} 〜 {END_DATE}\n初期資金: {INITIAL_CAPITAL:,} 円\n")

    tickers = load_tickers_from_csv()
    if BENCHMARK_TICKER not in tickers: tickers.append(BENCHMARK_TICKER)
    
    print("1. データ取得中...")
    df_all = fetch_bigquery_data(target_date=END_DATE, lookback_days=500)
    dict_dfs = {t: g.set_index('Date').sort_index() for t, g in df_all.groupby('Ticker') if t in tickers}
    
    benchmark_df = dict_dfs[BENCHMARK_TICKER].loc[START_DATE:END_DATE]
    trading_days = benchmark_df.index.tolist()
    
    # ★ 事前一括計算（ここで選択した戦略の get_all_signals を呼ぶ）
    print(f"1.5 {selected['name']} シグナル事前計算中...")
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

        # --- 売却判定 ---
        sold_tickers = []
        for ticker, pos in positions.items():
            df = dict_dfs[ticker]
            if current_date not in df.index: continue
            td = df.loc[current_date]
            
            sma25 = td.get('SMA25', td['Close'])
            entry_price = pos['entry_price']
            tp_price = entry_price * TAKE_PROFIT_PCT
            sl_price = sma25 * STOP_LOSS_PCT
            
            sell_price, reason = None, ""
            if td['Open'] <= sl_price: sell_price, reason = td['Open'], "損切り(窓)"
            elif td['Open'] >= tp_price: sell_price, reason = td['Open'], "利食い(窓)"
            elif td['Low'] <= sl_price: sell_price, reason = sl_price, "損切り"
            elif td['High'] >= tp_price: sell_price, reason = tp_price, "利食い"

            if sell_price:
                profit = (sell_price - entry_price) * pos['shares']
                cash += sell_price * pos['shares']
                trade_history.append({'ticker': ticker, 'entry_date': pos['entry_date'], 'exit_date': today_str, 'profit': profit, 'reason': reason})
                sold_tickers.append(ticker)
                print(f"   💰 [売却] {today_str} : {ticker} {reason} {profit:>+,.0f}円")

        for t in sold_tickers: del positions[t]

        # --- 購入判定 (出来高順) ---
        buy_queue = []
        for ticker in candidates_for_tomorrow:
            if ticker in positions: continue
            df = dict_dfs[ticker]
            if current_date not in df.index: continue
            loc = df.index.get_loc(current_date)
            if loc > 0: buy_queue.append((ticker, df.iloc[loc-1]['Volume']))
        
        buy_queue.sort(key=lambda x: x[1], reverse=True)

        for ticker, _ in buy_queue:
            df = dict_dfs[ticker]
            prev_close = df.iloc[df.index.get_loc(current_date)-1]['Close']
            td = df.loc[current_date]
            
            buy_price = None
            if td['Open'] > prev_close: buy_price = td['Open']
            elif td['High'] > prev_close: buy_price = prev_close
            
            if buy_price:
                cost = buy_price * POSITION_LOT
                if cash >= cost:
                    cash -= cost
                    positions[ticker] = {'entry_price': buy_price, 'shares': POSITION_LOT, 'entry_date': today_str}
                    print(f"   🛒 [購入] {today_str} : {ticker} {buy_price:,.1f}円")

        # --- 次の日のための抽出 ---
        candidates_for_tomorrow = [t for t, df in dict_dfs.items() if t != BENCHMARK_TICKER and current_date in df.index and df.at[current_date, 'is_buy_signal']]

        # --- 資産記録 ---
        current_eq = cash + sum(dict_dfs[t].loc[current_date, 'Close'] * p['shares'] for t, p in positions.items() if current_date in dict_dfs[t].index)
        daily_equity.append({'date': current_date, 'equity': current_eq})

    # --- 評価レポートとグラフ ---
    print("\n★★★ バックテスト評価レポート ★★★")
    equity_df = pd.DataFrame(daily_equity).set_index('date')
    final_eq = equity_df['equity'].iloc[-1]
    total_ret = (final_eq / INITIAL_CAPITAL) - 1
    
    print(f" ├ 最終総資産  : {final_eq:,.0f} 円")
    print(f" ├ 純利益      : {final_eq - INITIAL_CAPITAL:>+,.0f} 円 ({total_ret:>+5.1%})")

    # グラフ生成
    equity_df['benchmark'] = (benchmark_df['Close'] / benchmark_df['Close'].iloc[0]) * INITIAL_CAPITAL
    plt.figure(figsize=(10, 5)); plt.plot(equity_df['equity'], label='Strategy'); plt.plot(equity_df['benchmark'], label='TOPIX', color='gray', linestyle='--'); plt.legend(); plt.savefig("equity_curve.png")

if __name__ == "__main__":
    run_simulation()
