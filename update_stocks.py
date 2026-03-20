import os
import json
import io
import yfinance as yf
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload

# ==========================================
# 設定部分
# ==========================================
TARGET_FOLDER_ID = 'ここにあなたのフォルダIDを貼り付けます'

# GitHubリポジトリ内に置く銘柄リストのファイル名
CSV_LIST_PATH = 'tickers_list.csv' 
# ==========================================

def load_tickers_from_csv(file_path):
    """CSVから銘柄コードを読み込んでリスト化する関数"""
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

def get_drive_service():
    creds_json = os.environ.get('GCP_CREDENTIALS')
    if not creds_json:
        raise ValueError("合鍵（GCP_CREDENTIALS）が見つかりません。")
    creds_dict = json.loads(creds_json)
    credentials = service_account.Credentials.from_service_account_info(
        creds_dict, scopes=['https://www.googleapis.com/auth/drive']
    )
    return build('drive', 'v3', credentials=credentials)

def get_existing_files(service, folder_id):
    """【高速化】ドライブ内のファイル一覧を1回の通信でまとめて取得する"""
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

if __name__ == "__main__":
    print("株価データの自動取得を開始します...")
    
    target_tickers = load_tickers_from_csv(CSV_LIST_PATH)
    if not target_tickers:
        print(f"エラー: {CSV_LIST_PATH} が見つかりません。")
        exit()

    drive_service = get_drive_service()
    print("Googleドライブの既存ファイルリストを取得中...")
    existing_files = get_existing_files(drive_service, TARGET_FOLDER_ID)

    print(f"Yahoo Financeから {len(target_tickers)} 銘柄の過去2年分のデータを一括取得します（数分かかります）...")
    tickers_str = " ".join(target_tickers)
    new_data = yf.download(tickers_str, period="2y", group_by='ticker', progress=False)

    print("データを整形し、Googleドライブへ上書き保存します...")
    update_count = 0

    for ticker in target_tickers:
        try:
            # データの切り出し
            if len(target_tickers) == 1:
                df_new = new_data.dropna(how='all')
            elif ticker in new_data:
                df_new = new_data[ticker].dropna(how='all')
            else:
                continue

            if df_new.empty:
                continue

            # ------------------------------------------------
            # ★ ご自身で作成された整形ロジック（魔法の2行など）
            # ------------------------------------------------
            if isinstance(df_new.columns, pd.MultiIndex):
                df_new.columns = df_new.columns.get_level_values(0)

            df_new.index.name = 'Date'
            df_new.columns.name = None
            # ------------------------------------------------

            # CSVをメモリ上で作成してGoogleドライブへ送信
            csv_buffer = io.StringIO()
            df_new.to_csv(csv_buffer)
            media = MediaIoBaseUpload(io.BytesIO(csv_buffer.getvalue().encode('utf-8')), mimetype='text/csv', resumable=True)

            file_name = f"{ticker}.csv"

            # 既に存在する場合は上書き、無い場合は新規作成
            if file_name in existing_files:
                file_id = existing_files[file_name]
                drive_service.files().update(fileId=file_id, media_body=media).execute()
            else:
                file_metadata = {'name': file_name, 'parents': [TARGET_FOLDER_ID]}
                drive_service.files().create(body=file_metadata, media_body=media).execute()

            update_count += 1
            if update_count % 100 == 0:
                print(f" ... {update_count} 銘柄完了")

        except Exception as e:
            print(f" -> エラー発生 ({ticker}): {e}")

    print(f"日次更新が完了しました！ ({update_count} 銘柄)")
