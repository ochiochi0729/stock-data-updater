import os
import json
import yfinance as yf
import pandas as pd
import pandas_gbq
from google.oauth2 import service_account
import concurrent.futures

# ==========================================
# 設定部分
# ==========================================
PROJECT_ID = 'ここにあなたのプロジェクトIDを貼り付けます'  # ★変更必須
DATASET_ID = 'stock_db'
TABLE_ID = 'daily_prices'
CSV_LIST_PATH = 'tickers_list.csv'
# ==========================================

def get_credentials():
    creds_json = os.environ.get('GCP_CREDENTIALS')
    if not creds_json:
        raise ValueError("合鍵（GCP_CREDENTIALS）が見つかりません。")
    creds_dict = json.loads(creds_json)
    return service_account.Credentials.from_service_account_info(creds_dict)

def load_tickers_from_csv(file_path):
    if not os.path.exists(file_path):
        return []
    try:
        df = pd.read_csv(file_path)
        raw_codes = df.iloc[:, 0].astype(str)
        tickers = []
        for code in raw_codes:
            code = code.strip()
            if not code: continue
            if not code.upper().endswith('.T'):
                code = f"{code}.T"
            tickers.append(code.upper())
        return tickers
    except:
        return []

def fetch_single_ticker(ticker):
    try:
        df = yf.download(ticker, period="5d", progress=False)
        if df.empty:
            return None
        
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
            
        # ★ ここが今回の修正ポイント！：重複する列名があれば強制的に削除する
        df = df.loc[:, ~df.columns.duplicated()].copy()
            
        df.columns = df.columns.str.replace(' ', '_')
        df['Ticker'] = ticker
        df = df.reset_index()
        
        # ★ 念のための強化：日付の列名がズレていた場合、強制的に 'Date' に統一する
        df = df.rename(columns={'index': 'Date', 'Datetime': 'Date'})
        
        return df
    except Exception:
        return None

if __name__ == "__main__":
    print("【BigQuery特化・超爆速版】株価データの自動取得を開始します...")
    
    target_tickers = load_tickers_from_csv(CSV_LIST_PATH)
    if not target_tickers:
        print(f"エラー: {CSV_LIST_PATH} が見つかりません。")
        exit()

    print(f"Yahoo Financeから {len(target_tickers)} 銘柄の直近データを取得中（5スレッド並列）...")
    
    all_data = []
    completed_count = 0
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(fetch_single_ticker, ticker): ticker for ticker in target_tickers}
        
        for future in concurrent.futures.as_completed(futures):
            df_result = future.result()
            if df_result is not None:
                all_data.append(df_result)
                completed_count += 1
                
            if completed_count % 500 == 0:
                print(f" ... {completed_count} 銘柄のダウンロード完了")

    if not all_data:
        print("更新するデータがありませんでした。")
        exit()

    print("ダウンロード完了！データを結合してBigQueryへ一括送信します...")
    
    final_df = pd.concat(all_data, ignore_index=True)
    
    credentials = get_credentials()
    destination_table = f"{DATASET_ID}.{TABLE_ID}"
    
    try:
        pandas_gbq.to_gbq(
            final_df, 
            destination_table, 
            project_id=PROJECT_ID, 
            if_exists='append', 
            credentials=credentials
        )
        print(f"★ 大成功: 全 {completed_count} 銘柄のデータをBigQueryに書き込みました！")
    except Exception as e:
        print(f"BigQueryへの書き込みエラー: {e}")
