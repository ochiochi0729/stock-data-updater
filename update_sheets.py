import os
import pandas as pd
from google.cloud import bigquery
import gspread
from google.oauth2.service_account import Credentials
import json

# --- 設定 ---
PROJECT_ID = 'YOUR_PROJECT_ID'
DATASET_TABLE = 'YOUR_DATASET.YOUR_TABLE'
SPREADSHEET_ID = '1Tc7PZo0DgsHmQd-pCydAQukY9rFxfI2qw5MzSGJK5bg'
SHEET_NAME = 'シート１'

def run_export():
    # 1. 銘柄リストの読み込み (run_screener.pyが保存したものを想定)
    if not os.path.exists('tickers.csv'):
        print("抽出銘柄が見つかりません。")
        return
    tickers_df = pd.read_csv('tickers.csv')
    tickers = tickers_df['Ticker'].unique().tolist()
    
    if not tickers:
        print("銘柄リストが空です。")
        return

    # 2. BigQueryから1年分のデータを取得
    client = bigquery.Client()
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

    # 3. データのピボット変換 (縦持ち -> 横持ち)
    # Dateをインデックスにし、Tickerごとに各指標を列に展開
    df_pivoted = df.pivot(index='Date', columns='Ticker')
    
    # 列名を「銘柄コード_データ種類」の形式に変換 (例: 7203_Close)
    # columnsはMultiIndex (指標, 銘柄) になっているので順番を入れ替えて結合
    df_pivoted.columns = [f"{ticker}_{metric}" for metric, ticker in df_pivoted.columns]
    df_pivoted = df_pivoted.reset_index()
    
    # Date型を文字列に変換（スプレッドシート送信用）
    df_pivoted['Date'] = df_pivoted['Date'].astype(str)

    # 4. Googleスプレッドシートへの書き込み
    # GitHub Actions上の環境変数から認証情報を取得
    creds_dict = json.loads(os.environ['GCP_CREDENTIALS'])
    scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    gc = gspread.authorize(creds)
    
    sh = gc.open_by_key(SPREADSHEET_ID)
    worksheet = sh.worksheet(SHEET_NAME)
    
    # シートをクリアして新しいデータを書き込み
    worksheet.clear()
    worksheet.update([df_pivoted.columns.values.tolist()] + df_pivoted.values.tolist())
    print(f"スプレッドシートの更新が完了しました。銘柄数: {len(tickers)}")

if __name__ == "__main__":
    run_export()
