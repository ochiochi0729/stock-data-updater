import sys
import pandas as pd
import numpy as np
from datetime import datetime
# ★ yfinance, os, time などの通信・待機用ライブラリを全削除！
from core import Logger, load_tickers_from_csv, fetch_bigquery_data
# ★ IndicatorCalculator も不要になったので削除！
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
    # フェーズ1：データのダウンロード（BigQueryから一瞬で取得）
    # ==========================================
    print("1. BigQueryから過去データを一括取得中... (数秒〜十数秒で終わります)")
    
    # END_DATEを基準に、過去500日分のデータを一括取得（25日線・75日線も計算済み！）
    df_all = fetch_bigquery_data(target_date=END_DATE, lookback_days=500, forward_days=0)
    
    # 対象銘柄だけに絞り込み、高速アクセス用の辞書を作成
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
    print("   (過去1年間の相場を1日ずつ疑似体験しています...)")

    for i, current_date in enumerate(trading_days):
        today_str = current_date.strftime('%Y-%m-%d')
        
        if i % 20 == 0 or i == len(trading_days) - 1:
            print(f"  📅 タイムトラベル中: {today_str} ({i+1}/{len(trading_days)}日目) | 現在の保有銘柄数: {len(positions)}")

        # -------------------------------------------------
        # 既存ポジションの売却（エグジット）
        # -------------------------------------------------
        sold_tickers = []
        for ticker, pos in positions.items():
            df = dict_dfs[ticker]
            if current_date not in df.index: continue
            
            today_data = df.loc[current_date]
            
            # BigQueryのSMA25を取得（万が一NaNの場合は自力計算でカバー）
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
                trade_history.append({'ticker': ticker, 'entry_date': pos['entry_date'], 'exit_date': today_str, 'entry_price': entry_price, 'exit_price': sell_price, 'profit': profit, 'reason': reason})
                sold_tickers.append(ticker)

        for t in sold_tickers:
            del positions[t]

        # -------------------------------------------------
        # 新規銘柄の購入（エントリー）- 虫眼鏡モード
        # -------------------------------------------------
        valid_candidates = []
        for ticker in candidates_for_tomorrow:
            if ticker in positions: continue 
            df = dict_dfs[ticker]
            
            if current_date not in df.index:
                continue
                
            prev_idx = df.index.get_loc(current_date) - 1
            if prev_idx >= 0:
                valid_candidates.append((ticker, df.iloc[prev_idx].get('Volume', 0)))
        
        valid_candidates.sort(key=lambda x: x[1], reverse=True) 

        for ticker, _ in valid_candidates:
            df = dict_dfs[ticker]
            if current_date not in df.index: continue
            
            prev_idx = df.index.get_loc(current_date) - 1
            if prev_idx < 0: continue
                
            prev_close = float(df.iloc[prev_idx].get('Close', np.nan))
            today_data = df.loc[current_date]
            
            sma25 = today_data.get('SMA25')
            if pd.isna(sma25):
                sma25 = float(df.loc[:current_date, 'Close'].tail(25).mean())
                
            t_open = float(today_data.get('Open', np.nan))
            t_high = float(today_data.get('High', np.nan))
            t_low = float(today_data.get('Low', np.nan))
            
            print(f"    🔍 [購入審査] {today_str} : {ticker}")
            print(f"       前日終値:{prev_close:.1f}, 始値:{t_open:.1f}, 高値:{t_high:.1f}, 25日線:{sma25:.1f}")

            if pd.isna(sma25) or pd.isna(t_open) or pd.isna(prev_close): 
                print("       => ❌ データ欠損のため見送り")
                continue

            buy_price = None
            if t_open > prev_close:
                buy_price = t_open
                print(f"       => ⭕ 条件①クリア！ 始値({buy_price}円)で購入決定！")
            elif t_open <= prev_close and t_high > prev_close:
                buy_price = prev_close
                print(f"       => ⭕ 条件②クリア！ 前日終値({buy_price}円)で購入決定！")
            else:
                print("       => ❌ 条件未達 (寄り付き安く、日中も前日終値を超えず) のため見送り")
                
            if buy_price is not None:
                cost = buy_price * POSITION_LOT
                if cash >= cost:
                    cash -= cost
                    positions[ticker] = {'entry_price': buy_price, 'shares': POSITION_LOT, 'entry_date': today_str}
                    print(f"       💰 資金確保OK！ {ticker} を {POSITION_LOT}株 購入！(残金: {cash:,.0f}円)")
                    
                    sl_price = sma25 * STOP_LOSS_PCT
                    if t_low <= sl_price:
                        cash += sl_price * POSITION_LOT
                        profit = (sl_price - buy_price) * POSITION_LOT
                        trade_history.append({'ticker': ticker, 'entry_date': today_str, 'exit_date': today_str, 'entry_price': buy_price, 'exit_price': sl_price, 'profit': profit, 'reason': "損切り(即日)"})
                        del positions[ticker]
                        print(f"       😭 しかし、買ったその日に急落し、即日損切り({sl_price:.1f}円)されました...")
                else:
                    print(f"       💸 残念！資金不足のため購入できませんでした (必要:{cost}円, 残金:{cash:,.0f}円)")

        # -------------------------------------------------
        # 明日のためのスクリーニング
        # -------------------------------------------------
        candidates_for_tomorrow = []
        for ticker in dict_dfs.keys():
            if ticker == BENCHMARK_TICKER: continue
            
            df = dict_dfs[ticker]
            sub_df = df.loc[:current_date]
            if len(sub_df) < 100: continue 
            
            try:
                if screener.check_conditions(sub_df):
                    candidates_for_tomorrow.append(ticker)
                    print(f"  🎯 {current_date.strftime('%Y-%m-%d')}: {ticker} を明日の購入候補として抽出！")
            except Exception as e:
                print(f"\n🚨 スクリーニング内部で致命的なエラーが発生しました！")
                print(f"  エラー発生日: {current_date.strftime('%Y-%m-%d')}")
                print(f"  エラー銘柄: {ticker}")
                print(f"  エラー詳細: {e}")
                sys.exit(1) 

        # -------------------------------------------------
        # 資産の記録
        # -------------------------------------------------
        current_equity = cash
        for ticker, pos in positions.items():
            if current_date in dict_dfs[ticker].index:
                close_price = dict_dfs[ticker].loc[current_date].get('Close')
                if not pd.isna(close_price):
                    current_equity += close_price * pos['shares']
                else:
                    current_equity += pos['entry_price'] * pos['shares']
            else:
                current_equity += pos['entry_price'] * pos['shares'] 
                
        daily_equity.append({'date': current_date, 'equity': current_equity})

    # ==========================================
    # フェーズ3：結果出力
    # ==========================================
    print("\n3. シミュレーション完了！結果を計算します...\n")
    print(f"{'='*60}\n★★★ バックテスト評価レポート ★★★\n{'='*60}")
    
    equity_df = pd.DataFrame(daily_equity).set_index('date')
    final_equity = equity_df['equity'].iloc[-1]
    total_return = (final_equity / INITIAL_CAPITAL) - 1
    
    equity_df['peak'] = equity_df['equity'].cummax()
    equity_df['drawdown'] = (equity_df['equity'] - equity_df['peak']) / equity_df['peak']
    mdd = equity_df['drawdown'].min()
    
    wins = [t for t in trade_history if t['profit'] > 0]
    losses = [t for t in trade_history if t['profit'] <= 0]
    
    win_rate = len(wins) / len(trade_history) if trade_history else 0
    gross_profit = sum(t['profit'] for t in wins)
    gross_loss = abs(sum(t['profit'] for t in losses))
    pf = gross_profit / gross_loss if gross_loss > 0 else float('inf')
    
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
        print(f"\n【直近の取引履歴 (サンプル10件)】")
        for t in trade_history[-10:]:
            print(f" [{t['entry_date']} -> {t['exit_date']}] {t['ticker']:<6s} | 損益: {t['profit']:>+,.0f}円 ({t['reason']})")

if __name__ == "__main__":
    run_simulation()
