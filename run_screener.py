import os
import sys
import pandas as pd
import mplfinance as mpf  # ★ チャート描画用の魔法を追加
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
    
    # ★ 画像保存用のフォルダを作成
    os.makedirs("charts", exist_ok=True)
    
    for ticker in target_tickers:
        if ticker not in dict_dfs or len(dict_dfs[ticker]) == 0: continue
        df = dict_dfs[ticker]
        if screener_class.check_conditions(df):
            hit_tickers.append(ticker)
            print(f"★ 抽出: {ticker:<6s} | 本日終値: {df.iloc[-1]['Close']:,.1f}円")
            
            # ==========================================
            # ★ ここからチャート画像の自動生成処理
            # ==========================================
            try:
                # 直近100日分のデータを切り出す
                df_plot = df.iloc[-100:].copy()
                
                # 日本の一般的な証券会社カラー（陽線:赤、陰線:青）に設定
                mc = mpf.make_marketcolors(up='r', down='b', edge='inherit', wick='inherit', volume='inherit')
                s = mpf.make_mpf_style(marketcolors=mc, gridstyle=':')
                
                # ファイル名（例：charts/①パーフェクト_1234.T.png）
                safe_name = strategy_name[:6] # 長すぎるので最初の6文字をとる
                filename = f"charts/{safe_name}_{ticker}.png"
                
                # ローソク足、出来高、移動平均線(25, 75)を描画して保存
                mpf.plot(
                    df_plot,
                    type='candle',
                    volume=True,
                    mav=(25, 75),
                    style=s,
                    title=f"{ticker} ({safe_name})",
                    savefig=dict(fname=filename, dpi=100, bbox_inches='tight')
                )
            except Exception as e:
                print(f"  [!] チャート生成エラー({ticker}): {e}")

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
