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
PROJECT_ID = 'stock-data-updater-490714'  # ★変更必須
DATASET_ID = 'stock_db'
TABLE_ID = 'daily_prices'
CSV_LIST_PATH = 'tickers_list.csv'
# ==========================================

def get_credentials():
    """GitHubのSecretから合鍵を取得する"""
    creds_json = os.environ.get('GCP_CREDENTIALS')
    if not creds_json:
        raise ValueError("合鍵（GCP_CREDENTIALS）が見つかりません。")
    creds_dict = json.loads(creds_json)
    return service_account.Credentials.from_service_account_info(creds_dict)

def load_tickers_from_csv(file_path):
    """銘柄リストを読み込む"""
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
    """1銘柄のデータを取得し、BigQuery用に整形する"""
    try:
        # 直近5日分を取得
        df = yf.download(ticker, period="5d", progress=False)
        if df.empty:
            return None
        
        # マルチインデックスの解除
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
            
        # 列名のスペースをアンダーバーに変換（Adj Close -> Adj_Close）
        df.columns = df.columns.str.replace(' ', '_')
        
        # Ticker列（銘柄コード）を追加
        df['Ticker'] = ticker
        
        # Dateをインデックスから通常の列に戻す
        df = df.reset_index()
        
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
    
    # 5人のロボットで一斉にダウンロード開始
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
    
    # ★ここが最大の爆速ポイント：全銘柄を1つの巨大な表に合体させる
    final_df = pd.concat(all_data, ignore_index=True)
    
    # BigQueryへ一撃でアップロード
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
