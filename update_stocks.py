import os
import json
import io
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

# マルチスレッド（並列処理）時にAPIの混線を防ぐための専用ボックス
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

def process_single_ticker(ticker, df_new, existing_files):
    """1銘柄分の差分更新とアップロードを行う（並列処理で呼ばれる関数）"""
    try:
        service = get_drive_service()
        file_name = f"{ticker}.csv"

        # 1. 既存データの読み込みと結合（差分更新）
        if file_name in existing_files:
            file_id = existing_files[file_name]
            # ドライブから既存のCSVをメモリ上に直接ダウンロード
            content = service.files().get_media(fileId=file_id).execute()
            df_local = pd.read_csv(io.BytesIO(content), index_col=0, parse_dates=True)
            
            # 新しい5日分のデータと結合し、重複を排除（新しいデータで上書き）
            df_combined = pd.concat([df_local, df_new])
            df_combined = df_combined[~df_combined.index.duplicated(keep='last')]
            df_combined.sort_index(inplace=True)
        else:
            df_combined = df_new.copy()

        # 2. 魔法の2行（空欄行を出さないための整形）
        df_combined.index.name = 'Date'
        df_combined.columns.name = None

        # 3. CSVをメモリ上で作成
        csv_buffer = io.StringIO()
        df_combined.to_csv(csv_buffer)
        media = MediaIoBaseUpload(io.BytesIO(csv_buffer.getvalue().encode('utf-8')), mimetype='text/csv', resumable=True)

        # 4. Googleドライブへ送信
        if file_name in existing_files:
            service.files().update(fileId=file_id, media_body=media).execute()
        else:
            file_metadata = {'name': file_name, 'parents': [TARGET_FOLDER_ID]}
            service.files().create(body=file_metadata, media_body=media).execute()

        return True, ticker
    except Exception as e:
        return False, f"{ticker}: {e}"

if __name__ == "__main__":
    print("【日次特化・爆速版】株価データの自動取得を開始します...")
    
    target_tickers = load_tickers_from_csv(CSV_LIST_PATH)
    if not target_tickers:
        print(f"エラー: {CSV_LIST_PATH} が見つかりません。")
        exit()

    # メインスレッド用のサービスでファイル一覧を取得
    main_service = get_drive_service()
    print("Googleドライブの既存ファイルリストを取得中...")
    existing_files = get_existing_files(main_service, TARGET_FOLDER_ID)

    # ★ 取得期間を「過去5日分」に特化
    print(f"Yahoo Financeから {len(target_tickers)} 銘柄の直近データ(5日分)を一括取得します...")
    tickers_str = " ".join(target_tickers)
    new_data = yf.download(tickers_str, period="5d", group_by='ticker', progress=False)

    print("データをマージしてGoogleドライブへ並列アップロード中...")
    
    # 並列処理に渡すタスクの準備
    tasks = []
    for ticker in target_tickers:
        if len(target_tickers) == 1:
            df_new = new_data.dropna(how='all')
        elif ticker in new_data:
            df_new = new_data[ticker].dropna(how='all')
        else:
            continue
            
        if df_new.empty:
            continue
            
        if isinstance(df_new.columns, pd.MultiIndex):
            df_new.columns = df_new.columns.get_level_values(0)
            
        tasks.append((ticker, df_new))

    update_count = 0
    
    # ★ 5スレッドで並列処理を実行（一気に高速化！）
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(process_single_ticker, t[0], t[1], existing_files) for t in tasks]
        
        for future in concurrent.futures.as_completed(futures):
            success, result_msg = future.result()
            if success:
                update_count += 1
                if update_count % 100 == 0:
                    print(f" ... {update_count} 銘柄完了")
            else:
                print(f" -> エラー発生: {result_msg}")

    print(f"日次更新が完了しました！ ({update_count} 銘柄)")
