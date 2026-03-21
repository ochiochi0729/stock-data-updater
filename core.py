import os, sys, json
import pandas as pd
import pandas_gbq
from google.oauth2 import service_account

# --- 共通設定 ---
PROJECT_ID = 'stock-data-updater-490714'
DATASET_ID = 'stock_db'
VIEW_ID = 'clean_daily_prices'
CSV_LIST_PATH = 'tickers_list.csv'
BENCHMARK_TICKER = '1306.T'
EVAL_DAYS = [3, 6, 7, 10, 13, 16, 19, 22, 25, 28]
STOP_LOSS_THRESHOLD = -0.05

class Logger:
    def __init__(self, filename):
        self.terminal = sys.stdout
        self.log = open(filename, "w", encoding='utf-8')
    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)
    def flush(self):
        self.terminal.flush()
        self.log.flush()

def get_credentials():
    creds_json = os.environ.get('GCP_CREDENTIALS')
    if not creds_json: raise ValueError("GCP_CREDENTIALSが見つかりません。")
    return service_account.Credentials.from_service_account_info(json.loads(creds_json))

def load_tickers_from_csv():
    if not os.path.exists(CSV_LIST_PATH): return []
    try:
        df = pd.read_csv(CSV_LIST_PATH)
        return [c.strip().upper() + '.T' if not c.strip().upper().endswith('.T') else c.strip().upper() for c in df.iloc[:, 0].astype(str) if c.strip()]
    except: return []

def fetch_bigquery_data(target_date=None, lookback_days=600, forward_days=45):
    print("BigQueryから株価データを一括ダウンロード中...")
    try:
        if target_date:
            target_dt = pd.to_datetime(target_date)
            start_str = (target_dt - pd.Timedelta(days=lookback_days)).strftime('%Y-%m-%d')
            end_str = (target_dt + pd.Timedelta(days=forward_days)).strftime('%Y-%m-%d')
            query = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.{VIEW_ID}` WHERE Date >= '{start_str}' AND Date <= '{end_str}'"
        else:
            query = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.{VIEW_ID}` WHERE Date >= DATE_SUB(CURRENT_DATE('Asia/Tokyo'), INTERVAL {lookback_days} DAY)"
            
        df_all = pandas_gbq.read_gbq(query, project_id=PROJECT_ID, credentials=get_credentials())
        df_all['Date'] = pd.to_datetime(df_all['Date'])
        return df_all
    except Exception as e:
        print(f"BigQueryエラー: {e}")
        sys.exit(1)
