import os
import time
import pandas as pd
import yfinance as yf
import pandas_gbq
from core import load_tickers_from_csv
# もし core.py に BENCHMARK_TICKER や GCP認証情報 をまとめている場合は適宜インポートしてください

# ==========================================
# ★ データベース設定（ご自身の環境に合わせて変更してください）
# ==========================================
PROJECT_ID = "your-gcp-project-id"        # 例: "my-stock-project-123"
DATASET_TABLE = "dataset_name.table_name" # 例: "stock_data.daily_prices"
BENCHMARK_TICKER = "1306.T"               # TOPIX連動ETFなど

def update_all_stocks():
    print(f"{'='*60}\n▼ 株価データ更新（全件洗い替え：株式分割対応）\n{'='*60}")
    
    tickers = load_tickers_from_csv()
    if BENCHMARK_TICKER not in tickers:
        tickers.append(BENCHMARK_TICKER)
        
    print(f"対象: 全 {len(tickers)} 銘柄 | 取得期間: 過去5年分")
    
    all_data = []
    
    # 1銘柄ずつデータを取得
    for i, ticker in enumerate(tickers):
        try:
            # history() を使うことで、株式分割や配当落ちが自動的に調整されたデータが返ります
            ticker_obj = yf.Ticker(ticker)
            df = ticker_obj.history(period="5y")
            
            if df.empty:
                continue
                
            df = df.reset_index()
            
            # BigQueryへのアップロードエラーを防ぐため、Date列のタイムゾーン情報を削除
            if df['Date'].dt.tz is not None:
                df['Date'] = df['Date'].dt.tz_localize(None)
            
            df['Ticker'] = ticker
            
            # 必要なカラムだけを抽出（元のデータベーススキーマと完全一致させます）
            df = df[['Date', 'Ticker', 'Open', 'High', 'Low', 'Close', 'Volume']].copy()
            
            # ---------------------------------------------------------
            # ★ 整合性確保：各戦略で使う移動平均線をここで計算してDBに保存します
            # ---------------------------------------------------------
            df['SMA25'] = df['Close'].rolling(window=25).mean()
            df['SMA75'] = df['Close'].rolling(window=75).mean()
            df['SMA200'] = df['Close'].rolling(window=200).mean() # CwH用にこれも保存しておくと安全です
            
            all_data.append(df)
            
            # Yahoo Financeのアクセス制限（IPブロック）を回避するためのウェイト
            time.sleep(0.1)
            
            # 進捗表示
            if (i + 1) % 100 == 0:
                print(f" {i + 1} / {len(tickers)} 銘柄取得完了...")
                
        except Exception as e:
            print(f" ⚠️ {ticker} の取得に失敗: {e}")
            
    if not all_data:
        print("エラー: 取得できたデータがありません。")
        return
        
    # 全銘柄のデータを1つの巨大な表（DataFrame）に結合
    final_df = pd.concat(all_data, ignore_index=True)
    
    print(f"\n総データ件数: {len(final_df):,} 行")
    print("BigQueryへデータをアップロード（全件上書き）しています...")
    
    # ---------------------------------------------------------
    # ★ BigQueryへ書き込み (if_exists='replace' で過去の分割前データを完全に消去)
    # ※ credentials の設定が必要な場合は、既存のコードに合わせて追加してください
    # ---------------------------------------------------------
    pandas_gbq.to_gbq(
        final_df, 
        destination_table=DATASET_TABLE, 
        project_id=PROJECT_ID, 
        if_exists='replace'
    )
    
    print("\n✅ すべての更新が完了しました！")

if __name__ == "__main__":
    update_all_stocks()
