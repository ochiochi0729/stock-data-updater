import os
import sys
import json
import pandas as pd
import yfinance as yf
import pandas_gbq
import time
from google.oauth2 import service_account

# ==========================================
# 設定部分
# ==========================================
PROJECT_ID = 'stock-data-updater-490714'
DATASET_ID = 'stock_db'
TABLE_ID = 'daily_prices'
CSV_LIST_PATH = 'tickers_list.csv'

# ==========================================
# ログ出力用クラス
# ==========================================
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

sys.stdout = Logger("update_report.txt")

# ==========================================
# ヘルパー関数
# ==========================================
def get_credentials():
    creds_json = os.environ.get('GCP_CREDENTIALS')
    if not creds_json: raise ValueError("合鍵が見つかりません。")
    return service_account.Credentials.from_service_account_info(json.loads(creds_json))

def load_tickers_from_csv():
    if not os.path.exists(CSV_LIST_PATH): return []
    try:
        df = pd.read_csv(CSV_LIST_PATH, header=None)
        return [c.strip().upper() + '.T' if not c.strip().upper().endswith('.T') else c.strip().upper() for c in df.iloc[:, 0].astype(str) if c.strip()]
    except: return []

# ==========================================
# メイン処理
# ==========================================
def main():
    print(f"{'='*60}\n▼ 株価データベース更新レポート (SMA計算機能付き)\n{'='*60}")
    
    target_tickers = load_tickers_from_csv()
    if not target_tickers:
        print("エラー: 銘柄リストが見つかりません。")
        return

    creds = get_credentials()

    print("BigQueryの登録済み銘柄をチェック中...")
    try:
        query = f"SELECT DISTINCT Ticker FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`"
        df_existing = pandas_gbq.read_gbq(query, project_id=PROJECT_ID, credentials=creds, use_bqstorage_api=True)
        existing_tickers = set(df_existing['Ticker'].tolist())
    except Exception as e:
        print(f"⚠️ 登録済み銘柄の取得に失敗したため、全件新規とみなします。\n")
        existing_tickers = set()

    new_tickers = [t for t in target_tickers if t not in existing_tickers]
    update_tickers = [t for t in target_tickers if t in existing_tickers]

    print(f"\n[対象銘柄の振り分け結果]")
    print(f" ├ 既存銘柄の差分更新 (5日分切出): {len(update_tickers)}件")
    print(f" └ 新規銘柄の過去取得 (3年分): {len(new_tickers)}件\n")

    failed_tickers = []
    all_dfs = []  

    def download_data(tickers, period, desc, slice_days=None):
        if not tickers: return
        
        batch_size = 100 
        for i in range(0, len(tickers), batch_size):
            batch = tickers[i:i+batch_size]
            print(f"[{desc}] データ取得＆計算中... ({i+1}〜{min(i+batch_size, len(tickers))}件 / {len(tickers)}件)")
            
            yf.shared._ERRORS = {}
            # auto_adjust=False で配当落ちのおせっかい自動調整をオフ
            data = yf.download(batch, period=period, group_by='ticker', threads=True, progress=False, auto_adjust=False)
            time.sleep(1) 
            
            if yf.shared._ERRORS:
                for t, err in yf.shared._ERRORS.items():
                    err_str = str(err).split(':')[-1].strip()
                    failed_tickers.append((t, err_str))
            
            for t in batch:
                df_t = pd.DataFrame()
                if len(batch) == 1:
                    df_t = data.copy()
                elif t in data.columns.levels[0]:
                    df_t = data[t].copy()
                
                df_t = df_t.dropna(how='all')
                if not df_t.empty:
                    df_t = df_t.reset_index()
                    df_t['Ticker'] = t
                    df_t = df_t.sort_values('Date')
                    
                    # ★ここでデータベース保存前に25日線と75日線を自力計算！
                    df_t['SMA25'] = df_t['Close'].rolling(window=25).mean()
                    df_t['SMA75'] = df_t['Close'].rolling(window=75).mean()
                    
                    # ★既存更新の場合は、計算が終わった後に「最新の5日分」だけ残して尻尾を切る
                    if slice_days is not None:
                        df_t = df_t.tail(slice_days)
                    
                    # SMA25とSMA75の列も保存対象に含める
                    cols = [c for c in ['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'SMA25', 'SMA75', 'Ticker'] if c in df_t.columns]
                    all_dfs.append(df_t[cols])

    # ★ 既存更新時は「半年分(6mo)」取得して計算し、DBには「5日分」だけ保存する
    download_data(update_tickers, "6mo", "既存更新", slice_days=5)
    
    # ★ 新規追加時は「3年分(3y)」取得して全データ保存する
    download_data(new_tickers, "3y", "新規追加", slice_days=None)

    df_to_upload = pd.concat(all_dfs, ignore_index=True) if all_dfs else pd.DataFrame()

    if not df_to_upload.empty:
        df_to_upload['Date'] = pd.to_datetime(df_to_upload['Date']).dt.tz_localize(None)
        df_to_upload.columns = df_to_upload.columns.astype(str)
        print(f"\nBigQueryへデータをアップロード中... (総件数: {len(df_to_upload):,}行)")
        try:
            pandas_gbq.to_gbq(
                df_to_upload,
                f"{DATASET_ID}.{TABLE_ID}",
                project_id=PROJECT_ID,
                credentials=creds,
                if_exists='append'
            )
            print("✅ アップロード成功！")
        except Exception as e:
            print(f"❌ アップロード失敗: {e}")
            print("【ヒント】BigQueryのテーブルの列構成が変わったためエラーになった可能性があります。BigQuery上でテーブルを一度削除して再実行してください。")
    else:
        print("\n⚠️ アップロードするデータがありませんでした。")

    print("\n[取得失敗（エラー）レポート]")
    if failed_tickers:
        print(f" ❌ {len(failed_tickers)}件の銘柄でデータが取得できませんでした。")
        for t, reason in failed_tickers[:10]:
            print(f"  - {t:<6s} : {reason}")
        if len(failed_tickers) > 10:
            print(f"  ... 他 {len(failed_tickers) - 10} 件は省略")
    else:
        print(" ✅ 全銘柄のデータ取得に成功しました！")

if __name__ == "__main__":
    main()
