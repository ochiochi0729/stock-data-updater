import os
import pandas as pd
from google.cloud import bigquery
import gspread
from google.oauth2.service_account import Credentials
import json

# --- 設定 ---
PROJECT_ID = 'stock-data-updater-490714'  # ★ご自身のプロジェクトIDに書き換えてください
DATASET_TABLE = 'stock_db.daily_prices'  # ★ご自身のデータセットとテーブル名に書き換えてください
SPREADSHEET_ID = '1Tc7PZo0DgsHmQd-pCydAQukY9rFxfI2qw5MzSGJK5bg'

def run_export():
    # 0. 認証情報（鍵）の読み込み
    try:
        creds_dict = json.loads(os.environ['GCP_CREDENTIALS'])
        # スプレッドシートやドライブも操作できるようにスコープ（権限の範囲）を広めに設定
        scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/cloud-platform']
        credentials = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    except KeyError:
        print("エラー: 環境変数 'GCP_CREDENTIALS' が設定されていません。")
        return

    # 1. 銘柄リストの読み込み
    if not os.path.exists('tickers.csv'):
        print("抽出銘柄が見つかりません（本日の抽出は0件でした）。")
        return
    tickers_df = pd.read_csv('tickers.csv')
    tickers = tickers_df['Ticker'].unique().tolist()
    
    if not tickers:
        print("銘柄リストが空です。")
        return

    # 2. BigQueryから1年分のデータを取得
    # ★修正箇所：Clientに明確にcredentials（鍵）とprojectを渡す
    client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
    ticker_list_str = ", ".join([f"'{t}'" for t in tickers])
    
    query = f"""
    SELECT 
        Date, Ticker, Open, High, Low, Close, Volume, SMA25, SMA75, SMA200
    FROM `{DATASET_TABLE}`
    WHERE Ticker IN ({ticker_list_str})
      AND Date >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR)
    ORDER BY Date ASC
    """
    df = client.query(query).to_dataframe()

    if df.empty:
        print("BigQueryからデータが取得できませんでした。")
        return

    df = df.round(1)

    # 3. データのピボット変換 (縦持ち -> 横持ち)
    df_pivoted = df.pivot(index='Date', columns='Ticker')
    
    # 列名を「銘柄コード_データ種類」の形式に変換 (例: 7203_Close)
    df_pivoted.columns = [f"{ticker}_{metric}" for metric, ticker in df_pivoted.columns]
    df_pivoted = df_pivoted.reset_index()
    
    # Date型を文字列に変換（スプレッドシート送信用）
    df_pivoted['Date'] = df_pivoted['Date'].astype(str)

    # 4. Googleスプレッドシートへの書き込み
    # ★修正箇所：すでに読み込んだ credentials をそのまま使う
    gc = gspread.authorize(credentials)
    
    sh = gc.open_by_key(SPREADSHEET_ID)
    worksheet = sh.get_worksheet(0)
    
    # シートをクリアして新しいデータを書き込み
    worksheet.clear()
    worksheet.update([df_pivoted.columns.values.tolist()] + df_pivoted.values.tolist())
    print(f"スプレッドシートの更新が完了しました。対象銘柄数: {len(tickers)}")

if __name__ == "__main__":
    run_export()
