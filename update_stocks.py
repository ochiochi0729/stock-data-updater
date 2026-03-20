import os
import json
import io
import time
import yfinance as yf
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
import concurrent.futures
import threading

# ==========================================
# 設定部分
# ==========================================
TARGET_FOLDER_ID = '14UB8Owq0EpblGdY7P3CprB4AesnCqUc8'
CSV_LIST_PATH = 'tickers_list.csv' 
# ==========================================

thread_local = threading.local()

def get_drive_service():
    """スレッドごとに独立したGoogle Drive API接続を生成する"""
    if not hasattr(thread_local, "service"):
        creds_json = os.environ.get('GCP_CREDENTIALS')
        if not creds_json:
            raise ValueError("合鍵（GCP_CREDENTIALS）が見つかりません。")
        creds_dict = json.loads(creds_json)
        credentials = service_account.Credentials.from_service_account_info(
            creds_dict, scopes=['https://www.googleapis.com/auth/drive']
        )
        thread_local.service = build('drive', 'v3', credentials=credentials)
    return thread_local.service

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

def get_existing_files(service, folder_id):
    """Googleドライブ内のファイル一覧を一括取得"""
    existing_files = {}
    page_token = None
    while True:
        results = service.files().list(
            q=f"'{folder_id}' in parents and trashed=false",
            fields="nextPageToken, files(id, name)",
            pageToken=page_token,
            pageSize=1000
        ).execute()
        for file in results.get('files', []):
            existing_files[file['name']] = file['id']
        page_token = results.get('nextPageToken')
        if not page_token:
            break
    return existing_files

def process_single_ticker(ticker, existing_files):
    """1銘柄ごとに取得と更新を完結させる（並列処理用）"""
    try:
        file_name = f"{ticker}.csv"
        
        # ★ 修正1: ロボットの容量エラーを防ぐため、新規作成はせずスキップする
        if file_name not in existing_files:
            return True, f"{ticker}: 既存CSVなし(スキップ)"

        # ★ 修正2: Yahooのパンクを防ぐため、1銘柄ずつ個別に5日分を取得
        df_new = yf.download(ticker, period="5d", progress=False)
        
        if df_new.empty:
            return True, f"{ticker}: データなし(スキップ)"

        # マルチインデックスの解除
        if isinstance(df_new.columns, pd.MultiIndex):
            df_new.columns = df_new.columns.get_level_values(0)

        service = get_drive_service()
        file_id = existing_files[file_name]

        # 既存データのダウンロードと結合
        content = service.files().get_media(fileId=file_id).execute()
        df_local = pd.read_csv(io.BytesIO(content), index_col=0, parse_dates=True)
        
        df_combined = pd.concat([df_local, df_new])
        df_combined = df_combined[~df_combined.index.duplicated(keep='last')]
        df_combined.sort_index(inplace=True)

        # 魔法の2行（空欄行を出さないための整形）
        df_combined.index.name = 'Date'
        df_combined.columns.name = None

        # メモリ上でCSV化
        csv_buffer = io.StringIO()
        df_combined.to_csv(csv_buffer)
        media = MediaIoBaseUpload(io.BytesIO(csv_buffer.getvalue().encode('utf-8')), mimetype='text/csv', resumable=True)

        # ★ 既存ファイルの上書き更新のみ実行（ユーザーの容量を使うのでエラーにならない）
        service.files().update(fileId=file_id, media_body=media).execute()

        return True, f"{ticker}: 更新完了"
    except Exception as e:
        return False, f"{ticker} のエラー: {e}"

if __name__ == "__main__":
    print("【日次特化・完全安定版】株価データの自動取得を開始します...")
    
    target_tickers = load_tickers_from_csv(CSV_LIST_PATH)
    if not target_tickers:
        print(f"エラー: {CSV_LIST_PATH} が見つかりません。")
        exit()

    main_service = get_drive_service()
    print("Googleドライブの既存ファイルリストを取得中...")
    existing_files = get_existing_files(main_service, TARGET_FOLDER_ID)

    print(f"全 {len(target_tickers)} 銘柄の取得＆更新を開始します（5スレッド並列処理）...")
    
    update_count = 0
    skip_count = 0
    
    # 5人で手分けして1銘柄ずつ処理していく
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(process_single_ticker, ticker, existing_files) for ticker in target_tickers]
        
        for future in concurrent.futures.as_completed(futures):
            success, result_msg = future.result()
            if success:
                if "更新完了" in result_msg:
                    update_count += 1
                else:
                    skip_count += 1
                    
                total_processed = update_count + skip_count
                if total_processed % 100 == 0:
                    print(f" ... {total_processed} / {len(target_tickers)} 銘柄完了 (更新: {update_count}, スキップ: {skip_count})")
            else:
                print(f" -> {result_msg}")

    print(f"\nすべての処理が完了しました！ [更新成功: {update_count} 銘柄, スキップ: {skip_count} 銘柄]")
