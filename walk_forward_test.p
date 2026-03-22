import sys
import pandas as pd
import numpy as np
import yfinance as yf
from datetime import datetime
from core import Logger, load_tickers_from_csv
from strategies.indicators import IndicatorCalculator
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
    print(f"初期資金: {INITIAL_CAPITAL:,} 円 | 1銘柄: {POSITION_LOT}株")
    print(f"利食い: +{(TAKE_PROFIT_PCT-1)*100:.0f}% | 損切り: 25日線 × {STOP_LOSS_PCT}\n")

    tickers = load_tickers_from_csv()
    if BENCHMARK_TICKER not in tickers:
        tickers.append(BENCHMARK_TICKER)
        
    # テスト対象を絞る場合はここで調整（テストの高速化のため）
    # tickers = tickers[:200] 

    print("1. 過去データの取得とインジケーター計算中... (数分かかります)")
    # 25日線や75日線を計算するため、半年前(2024-06)からのデータを取得
    data = yf.download(tickers, start="2024-06-01", end="2026-01-10", group_by='ticker', auto_adjust=False, progress=False)
    
    dict_dfs = {}
    for t in tickers:
        try:
            df = data[t].copy() if len(tickers) > 1 else data.copy()
            df = df.dropna(how='all')
            if df.empty: continue
            # 指標の計算
            df = IndicatorCalculator.add_indicators(df)
            dict_dfs[t] = df
        except: pass

    # 営業日カレンダーの作成（TOPIX ETFの営業日を基準にする）
    if BENCHMARK_TICKER not in dict_dfs:
        print("エラー: ベンチマークデータが取得できませんでした。")
        return
    
    benchmark_df = dict_dfs[BENCHMARK_TICKER].loc[START_DATE:END_DATE]
    trading_days = benchmark_df.index.tolist()
    
    # 状態管理
    cash = INITIAL_CAPITAL
    positions = {} # {ticker: {'entry_price': price, 'shares': shares, 'entry_date': date}}
    trade_history = []
    daily_equity = []
    
    candidates_for_tomorrow = [] # 前日のスクリーニング結果
    screener = PerfectOrderScreener()

    print(f"2. シミュレーション開始 (全 {len(trading_days)} 営業日)\n")

    for i, current_date in enumerate(trading_days):
        today_str = current_date.strftime('%Y-%m-%d')
        
        # -------------------------------------------------
        # フェーズ1：既存ポジションの売却（エグジット）
        # -------------------------------------------------
        sold_tickers = []
        for ticker, pos in positions.items():
            df = dict_dfs[ticker]
            if current_date not in df.index: continue
            
            today_data = df.loc[current_date]
            t_open = today_data['Open']
            t_high = today_data['High']
            t_low = today_data['Low']
            t_close = today_data['Close']
            sma25 = today_data['SMA25']
            
            if pd.isna(sma25): continue
                
            entry_price = pos['entry_price']
            tp_price = entry_price * TAKE_PROFIT_PCT
            sl_price = sma25 * STOP_LOSS_PCT
            
            sell_price = None
            reason = ""
            
            # 1. 窓開けギャップダウン（始値で既に損切りライン割れ）
            if t_open <= sl_price:
                sell_price = t_open
                reason = "損切り(窓開け)"
            # 2. 窓開けギャップアップ（始値で既に利食いライン超え）
            elif t_open >= tp_price:
                sell_price = t_open
                reason = "利食い(窓開け)"
            # 3. 日中の損切りライン到達（保守的に最悪ケースを優先）
            elif t_low <= sl_price:
                sell_price = sl_price
                reason = "損切り(日中)"
            # 4. 日中の利食いライン到達
            elif t_high >= tp_price:
                sell_price = tp_price
                reason = "利食い(日中)"
                
            if sell_price is not None:
                profit = (sell_price - entry_price) * pos['shares']
                cash += sell_price * pos['shares']
                trade_history.append({
                    'ticker': ticker, 'entry_date': pos['entry_date'], 'exit_date': today_str,
                    'entry_price': entry_price, 'exit_price': sell_price, 'profit': profit, 'reason': reason
                })
                sold_tickers.append(ticker)

        for t in sold_tickers:
            del positions[t]

        # -------------------------------------------------
        # フェーズ2：新規銘柄の購入（エントリー）
        # -------------------------------------------------
        # 前日の候補銘柄を出来高順にソートして優先度を決める（資金不足対策）
        valid_candidates = []
        for ticker in candidates_for_tomorrow:
            if ticker in positions: continue # 既に持っている銘柄は買わない
            df = dict_dfs[ticker]
            prev_idx = df.index.get_loc(current_date) - 1
            if prev_idx >= 0:
                prev_vol = df.iloc[prev_idx]['Volume']
                valid_candidates.append((ticker, prev_vol))
        
        valid_candidates.sort(key=lambda x: x[1], reverse=True) # 出来高降順

        for ticker, _ in valid_candidates:
            df = dict_dfs[ticker]
            if current_date not in df.index: continue
            
            prev_idx = df.index.get_loc(current_date) - 1
            if prev_idx < 0: continue
                
            prev_close = df.iloc[prev_idx]['Close']
            today_data = df.loc[current_date]
            t_open, t_high, t_low, t_close, sma25 = today_data['Open'], today_data['High'], today_data['Low'], today_data['Close'], today_data['SMA25']
            
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
                    
                    # 【重要】即日エグジット判定（買ったその日に損切りラインを割ったか）
                    sl_price = sma25 * STOP_LOSS_PCT
                    if t_low <= sl_price:
                        # 即日損切り
                        cash += sl_price * POSITION_LOT
                        profit = (sl_price - buy_price) * POSITION_LOT
                        trade_history.append({
                            'ticker': ticker, 'entry_date': today_str, 'exit_date': today_str,
                            'entry_price': buy_price, 'exit_price': sl_price, 'profit': profit, 'reason': "損切り(即日)"
                        })
                        del positions[ticker]

        # -------------------------------------------------
        # フェーズ3：明日のためのスクリーニング
        # -------------------------------------------------
        candidates_for_tomorrow = []
        for ticker, df in dict_dfs.items():
            if ticker == BENCHMARK_TICKER: continue
            # 現在の日付までのデータで判定
            try:
                sub_df = df.loc[:current_date]
                if len(sub_df) > 0 and screener.check_conditions(sub_df):
                    candidates_for_tomorrow.append(ticker)
            except: pass

        # -------------------------------------------------
        # フェーズ4：日次資産の記録
        # -------------------------------------------------
        current_equity = cash
        for ticker, pos in positions.items():
            if current_date in dict_dfs[ticker].index:
                current_equity += dict_dfs[ticker].loc[current_date]['Close'] * pos['shares']
            else:
                current_equity += pos['entry_price'] * pos['shares'] # 取得できない場合は買値で計算
                
        daily_equity.append({'date': current_date, 'equity': current_equity})

    # ==========================================
    # 結果の評価とKPI計算
    # ==========================================
    print("\n3. シミュレーション完了！結果を計算します...\n")
    print(f"{'='*60}\n★★★ バックテスト評価レポート ★★★\n{'='*60}")
    
    # --- 資産・MDD計算 ---
    equity_df = pd.DataFrame(daily_equity).set_index('date')
    final_equity = equity_df['equity'].iloc[-1]
    total_return = (final_equity / INITIAL_CAPITAL) - 1
    
    equity_df['peak'] = equity_df['equity'].cummax()
    equity_df['drawdown'] = (equity_df['equity'] - equity_df['peak']) / equity_df['peak']
    mdd = equity_df['drawdown'].min()
    
    # --- 勝率・PF計算 ---
    wins = [t for t in trade_history if t['profit'] > 0]
    losses = [t for t in trade_history if t['profit'] <= 0]
    
    win_rate = len(wins) / len(trade_history) if trade_history else 0
    gross_profit = sum(t['profit'] for t in wins)
    gross_loss = abs(sum(t['profit'] for t in losses))
    pf = gross_profit / gross_loss if gross_loss > 0 else float('inf')
    
    # --- ベンチマーク計算 ---
    bench_start = benchmark_df['Close'].iloc[0]
    bench_end = benchmark_df['Close'].iloc[-1]
    bench_return = (bench_end / bench_start) - 1
    
    print(f"【総合パフォーマンス】")
    print(f" ├ 初期資金    : {INITIAL_CAPITAL:,.0f} 円")
    print(f" ├ 最終総資産  : {final_equity:,.0f} 円")
    print(f" ├ 純利益      : {final_equity - INITIAL_CAPITAL:>+,.0f} 円 ({total_return:>+5.1%})")
    print(f" ├ 最大DD(MDD) : {mdd:>+5.1%} (資産の最大落ち込み幅)")
    print(f" └ TOPIX比較   : {bench_return:>+5.1%} (ベンチマークの成績)")
    
    print(f"\n【トレード詳細】")
    print(f" ├ 総取引回数  : {len(trade_history)} 回 (現在保有中の銘柄は除く)")
    print(f" ├ 勝率        : {win_rate:>5.1%} ({len(wins)}勝 / {len(losses)}敗)")
    print(f" ├ 平均利益    : {gross_profit/len(wins) if wins else 0:>+,.0f} 円")
    print(f" ├ 平均損失    : {-gross_loss/len(losses) if losses else 0:>+,.0f} 円")
    print(f" └ プロフィット・ファクター (PF) : {pf:.2f}")

    if trade_history:
        print(f"\n【直近の取引履歴 (サンプル5件)】")
        for t in trade_history[-5:]:
            print(f" [{t['entry_date']} -> {t['exit_date']}] {t['ticker']:<6s} | 損益: {t['profit']:>+,.0f}円 ({t['reason']})")

if __name__ == "__main__":
    run_simulation()
