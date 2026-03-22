import sys
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime
from core import Logger, load_tickers_from_csv, fetch_bigquery_data
from strategies.perfect_order import PerfectOrderScreener

sys.stdout = Logger("walk_forward_report.txt")

# ==========================================
# ★ シミュレーション設定
# ==========================================
INITIAL_CAPITAL = 10_000_000  # 初期資金（1000万円）
POSITION_LOT = 100            # 1回の購入株数（100株）
TAKE_PROFIT_PCT = 1.20        # 利食いライン（買値の+20%）
STOP_LOSS_PCT = 0.97          # 損切りライン（25日線の-3%）

START_DATE = '2025-01-01'
END_DATE = '2025-12-31'
BENCHMARK_TICKER = '1306.T'   # TOPIX連動ETF

def run_simulation():
    print(f"{'='*60}\n▼ ウォークフォワード・テスト（時系列シミュレーション）\n{'='*60}")
    print(f"期間: {START_DATE} 〜 {END_DATE}")
    print(f"初期資金: {INITIAL_CAPITAL:,} 円 | 1銘柄: {POSITION_LOT}株\n")

    tickers = load_tickers_from_csv()
    if BENCHMARK_TICKER not in tickers:
        tickers.append(BENCHMARK_TICKER)
    valid_tickers = set(tickers)

    # ==========================================
    # フェーズ1：データのダウンロード
    # ==========================================
    print("1. BigQueryから過去データを一括取得中...")
    df_all = fetch_bigquery_data(target_date=END_DATE, lookback_days=500, forward_days=0)
    
    dict_dfs = {ticker: group.set_index('Date').sort_index() for ticker, group in df_all.groupby('Ticker') if ticker in valid_tickers}

    if BENCHMARK_TICKER not in dict_dfs:
        print("エラー: ベンチマークデータが取得できませんでした。")
        return
    
    benchmark_df = dict_dfs[BENCHMARK_TICKER].loc[START_DATE:END_DATE]
    trading_days = benchmark_df.index.tolist()
    
    cash = INITIAL_CAPITAL
    positions = {} 
    trade_history = []
    daily_equity = []
    candidates_for_tomorrow = [] 
    screener = PerfectOrderScreener()

    # ==========================================
    # フェーズ2：日次シミュレーション
    # ==========================================
    print(f"\n2. シミュレーション開始 (全 {len(trading_days)} 営業日)")

    for i, current_date in enumerate(trading_days):
        today_str = current_date.strftime('%Y-%m-%d')
        
        if i % 20 == 0 or i == len(trading_days) - 1:
            print(f"   📅 タイムトラベル中: {today_str} ({i+1}/{len(trading_days)}日目) | 保有: {len(positions)}銘柄")

        # -------------------------------------------------
        # 既存ポジションの売却（エグジット）
        # -------------------------------------------------
        sold_tickers = []
        for ticker, pos in positions.items():
            df = dict_dfs[ticker]
            if current_date not in df.index: continue
            
            today_data = df.loc[current_date]
            sma25 = today_data.get('SMA25')
            if pd.isna(sma25):
                sma25 = df.loc[:current_date, 'Close'].tail(25).mean()
                
            t_open = today_data.get('Open')
            if pd.isna(sma25) or pd.isna(t_open): continue
                
            t_high = today_data.get('High')
            t_low = today_data.get('Low')
                
            entry_price = pos['entry_price']
            tp_price = entry_price * TAKE_PROFIT_PCT
            sl_price = sma25 * STOP_LOSS_PCT
            
            sell_price, reason = None, ""
            
            if t_open <= sl_price:
                sell_price, reason = t_open, "損切り(窓開け)"
            elif t_open >= tp_price:
                sell_price, reason = t_open, "利食い(窓開け)"
            elif t_low <= sl_price:
                sell_price, reason = sl_price, "損切り(日中)"
            elif t_high >= tp_price:
                sell_price, reason = tp_price, "利食い(日中)"
                
            if sell_price is not None:
                profit = (sell_price - entry_price) * pos['shares']
                cash += sell_price * pos['shares']
                trade_history.append({
                    'ticker': ticker, 'entry_date': pos['entry_date'], 'exit_date': today_str, 
                    'entry_price': entry_price, 'exit_price': sell_price, 'profit': profit, 'reason': reason
                })
                sold_tickers.append(ticker)
                # ★売却報告の追加
                print(f"   💰 [売却完了] {today_str} : {ticker} を {sell_price:,.1f}円 で決済 (理由: {reason})")
                print(f"       損益: {profit:>+,.0f}円 | 確定後の残金: {cash:,.0f}円")

        for t in sold_tickers:
            del positions[t]

        # -------------------------------------------------
        # 新規銘柄の購入（エントリー）
        # -------------------------------------------------
        valid_candidates = []
        for ticker in candidates_for_tomorrow:
            if ticker in positions: continue 
            df = dict_dfs[ticker]
            if current_date not in df.index: continue
            prev_idx = df.index.get_loc(current_date) - 1
            if prev_idx >= 0:
                valid_candidates.append((ticker, df.iloc[prev_idx].get('Volume', 0)))
        
        valid_candidates.sort(key=lambda x: x[1], reverse=True) 

        for ticker, _ in valid_candidates:
            df = dict_dfs[ticker]
            prev_idx = df.index.get_loc(current_date) - 1
            if prev_idx < 0: continue
                
            prev_close = float(df.iloc[prev_idx].get('Close', np.nan))
            today_data = df.loc[current_date]
            sma25 = today_data.get('SMA25')
            if pd.isna(sma25): sma25 = float(df.loc[:current_date, 'Close'].tail(25).mean())
                
            t_open = float(today_data.get('Open', np.nan))
            t_high = float(today_data.get('High', np.nan))
            t_low = float(today_data.get('Low', np.nan))
            
            buy_price = None
            if t_open > prev_close:
                buy_price = t_open
            elif t_open <= prev_close and t_high > prev_close:
                buy_price = prev_close
                
            if buy_price is not None:
                cost = buy_price * POSITION_LOT
                if cash >= cost:
                    cash -= cost
                    positions[ticker] = {'entry_price': buy_price, 'shares': POSITION_LOT, 'entry_date': today_str}
                    print(f"   🛒 [購入完了] {today_str} : {ticker} を {buy_price:,.1f}円 で購入 (残金: {cash:,.0f}円)")
                    
                    # 即日損切り判定
                    sl_price = sma25 * STOP_LOSS_PCT
                    if t_low <= sl_price:
                        cash += sl_price * POSITION_LOT
                        profit = (sl_price - buy_price) * POSITION_LOT
                        trade_history.append({
                            'ticker': ticker, 'entry_date': today_str, 'exit_date': today_str, 
                            'entry_price': buy_price, 'exit_price': sl_price, 'profit': profit, 'reason': "損切り(即日)"
                        })
                        del positions[ticker]
                        print(f"   ⚠️ [即日決済] {ticker} が購入直後に急落。{sl_price:.1f}円 で損切りされました。")

        # -------------------------------------------------
        # 明日のためのスクリーニング
        # -------------------------------------------------
        candidates_for_tomorrow = []
        for ticker in dict_dfs.keys():
            if ticker == BENCHMARK_TICKER: continue
            df = dict_dfs[ticker]
            sub_df = df.loc[:current_date]
            if len(sub_df) < 100: continue 
            
            if screener.check_conditions(sub_df):
                candidates_for_tomorrow.append(ticker)

        # -------------------------------------------------
        # 資産の記録
        # -------------------------------------------------
        current_equity = cash
        for ticker, pos in positions.items():
            if current_date in dict_dfs[ticker].index:
                close_price = dict_dfs[ticker].loc[current_date].get('Close')
                current_equity += (close_price if not pd.isna(close_price) else pos['entry_price']) * pos['shares']
            else:
                current_equity += pos['entry_price'] * pos['shares'] 
                
        daily_equity.append({'date': current_date, 'equity': current_equity})

    # ==========================================
    # フェーズ3：結果出力とグラフ生成
    # ==========================================
    equity_df = pd.DataFrame(daily_equity).set_index('date')
    final_equity = equity_df['equity'].iloc[-1]
    
    # ベンチマーク（TOPIX）の資産推移も計算
    bench_initial_price = benchmark_df['Close'].iloc[0]
    equity_df['benchmark_equity'] = (benchmark_df['Close'] / bench_initial_price) * INITIAL_CAPITAL

    # --- 評価レポート出力 ---
    print("\n" + "="*60 + "\n★★★ バックテスト評価レポート ★★★\n" + "="*60)
    total_return = (final_equity / INITIAL_CAPITAL) - 1
    bench_return = (benchmark_df['Close'].iloc[-1] / bench_initial_price) - 1
    
    print(f"【総合パフォーマンス】")
    print(f" ├ 最終総資産  : {final_equity:,.0f} 円")
    print(f" ├ 純利益      : {final_equity - INITIAL_CAPITAL:>+,.0f} 円 ({total_return:>+5.1%})")
    print(f" └ TOPIX比較   : {bench_return:>+5.1%} (ベンチマーク)")

    # --- グラフの生成と保存 ---
    plt.figure(figsize=(12, 6))
    plt.plot(equity_df.index, equity_df['equity'], label='Simulation (My Strategy)', color='#1f77b4', linewidth=2)
    plt.plot(equity_df.index, equity_df['benchmark_equity'], label=f'Benchmark ({BENCHMARK_TICKER})', color='#7f7f7f', linestyle='--', alpha=0.7)
    
    plt.title('Walk-Forward Test: Equity Curve Comparison', fontsize=14)
    plt.xlabel('Date', fontsize=12)
    plt.ylabel('Total Assets (JPY)', fontsize=12)
    plt.legend()
    plt.grid(True, which='both', linestyle='--', alpha=0.5)
    plt.tight_layout()
    
    graph_filename = "equity_curve.png"
    plt.savefig(graph_filename)
    print(f"\n📈 資産推移グラフを '{graph_filename}' として出力しました。")

if __name__ == "__main__":
    run_simulation()
