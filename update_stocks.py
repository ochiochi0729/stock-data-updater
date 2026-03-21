import os
import sys
import json
import pandas as pd
import yfinance as yf
import pandas_gbq
from google.oauth2 import service_account

# ==========================================
# 設定部分
# ==========================================
PROJECT_ID = 'stock-data-updater-490714'
DATASET_ID = 'stock_db'
TABLE_ID = 'daily_prices'  # ★生のデータを入れるテーブル（ビューではありません）
CSV_LIST_PATH = 'tickers_list.csv'

# ==========================================
# ログ出力用クラス（メール送信用）
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
        df = pd.read_csv(CSV_LIST_PATH)
        return [c.strip().upper() + '.T' if not c.strip().upper().endswith('.T') else c.strip().upper() for c in df.iloc[:, 0].astype(str) if c.strip()]
    except: return []

# ==========================================
# メイン処理
# ==========================================
def main():
    print(f"{'='*60}\n▼ 株価データベース更新レポート\n{'='*60}")
    
    target_tickers = load_tickers_from_csv()
    if not target_tickers:
        print("エラー: 銘柄リストが見つかりません。")
        return

    creds = get_credentials()

    # 1. BigQueryから「既に登録されている銘柄」のリストを取得
    print("BigQueryの登録済み銘柄をチェック中...")
    try:
        query = f"SELECT DISTINCT Ticker FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`"
        df_existing = pandas_gbq.read_gbq(query, project_id=PROJECT_ID, credentials=creds)
        existing_tickers = set(df_existing['Ticker'].tolist())
    except Exception as e:
        print(f"⚠️ 登録済み銘柄の取得に失敗したため、全件新規とみなします。\n")
        existing_tickers = set()

    # 2. 自動振り分け
    new_tickers = [t for t in target_tickers if t not in existing_tickers]
    update_tickers = [t for t in target_tickers if t in existing_tickers]

    print(f"\n[対象銘柄の振り分け結果]")
    print(f" ├ 既存銘柄の差分更新 (5日分): {len(update_tickers)}件")
    print(f" └ 新規銘柄の過去取得 (1年分): {len(new_tickers)}件\n")

    failed_tickers = []
    df_to_upload = pd.DataFrame()

    # 3. データ取得用関数（バッチ処理）
    def download_data(tickers, period, desc):
        nonlocal df_to_upload
        if not tickers: return
        
        batch_size = 500  # 大量取得によるエラーを防ぐため500件ずつ
        for i in range(0, len(tickers), batch_size):
            batch = tickers[i:i+batch_size]
            print(f"[{desc}] データ取得中... ({i+1}〜{min(i+batch_size, len(tickers))}件 / {len(tickers)}件)")
            
            # yfinanceの内部エラー記録をリセット
            yf.shared._ERRORS = {}
            data = yf.download(batch, period=period, group_by='ticker', threads=True, progress=False)
            
            # 失敗した銘柄をキャッチして記録
            if yf.shared._ERRORS:
                for t, err in yf.shared._ERRORS.items():
                    # 短くて分かりやすいエラー名に変換
                    err_str = str(err).split(':')[-1].strip()
                    failed_tickers.append((t, err_str))
            
            # ダウンロードしたデータを成形
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
                    cols = [c for c in ['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Ticker'] if c in df_t.columns]
                    df_to_upload = pd.concat([df_to_upload, df_t[cols]], ignore_index=True)

    # 4. 実行
    download_data(update_tickers, "5d", "既存更新")
    download_data(new_tickers, "1y", "新規追加")

    # 5. BigQueryへアップロード
    if not df_to_upload.empty:
        df_to_upload['Date'] = pd.to_datetime(df_to_upload['Date']).dt.tz_localize(None)
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
    else:
        print("\n⚠️ アップロードするデータがありませんでした。")

    # 6. エラーサマリーの出力
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
