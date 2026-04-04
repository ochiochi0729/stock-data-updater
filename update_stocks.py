import os
import sys
import json
import pandas as pd
import yfinance as yf
import pandas_gbq
import time
from google.oauth2 import service_account

# ==========================================
# 設定部分 (ご提示の以前の設定を継承)
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
# ヘルパー関数[cite: 4]
# ==========================================
def get_credentials():
    # GitHub ActionsのSecretsに設定された環境変数から認証情報を取得[cite: 4]
    creds_json = os.environ.get('GCP_CREDENTIALS')
    if not creds_json: raise ValueError("GCPの認証情報(GCP_CREDENTIALS)が見つかりません。")
    return service_account.Credentials.from_service_account_info(json.loads(creds_json))

def load_tickers_from_csv():
    if not os.path.exists(CSV_LIST_PATH): return []
    try:
        df = pd.read_csv(CSV_LIST_PATH, header=None)
        # 銘柄コードを .T 形式に正規化[cite: 4]
        return [c.strip().upper() + '.T' if not c.strip().upper().endswith('.T') else c.strip().upper() for c in df.iloc[:, 0].astype(str) if c.strip()]
    except: return []

# ==========================================
# メイン処理
# ==========================================
def main():
    print(f"{'='*60}\n▼ 株価データベース更新レポート (株式分割対応・全件洗い替え)\n{'='*60}")
    
    target_tickers = load_tickers_from_csv()
    if not target_tickers:
        print("エラー: 銘柄リストが見つかりません。")
        return

    # 認証情報の取得[cite: 4]
    creds = get_credentials()
    
    # ベンチマーク(TOPIX)を追加
    BENCHMARK_TICKER = "1306.T"
    if BENCHMARK_TICKER not in target_tickers:
        target_tickers.append(BENCHMARK_TICKER)

    print(f"対象銘柄数: {len(target_tickers)}件")
    print(f"取得モード: 過去5年分を全件取得してBigQueryを上書き(replace)\n")

    failed_tickers = []
    all_dfs = []  

    # 1銘柄ずつ確実に取得（個別取得の方がエラー時のリトライやSMA計算が確実です）
    for i, ticker in enumerate(target_tickers):
        try:
            # yfinanceのhistory(period="5y")はデフォルトで株式分割・配当調整済みを返します
            data = yf.Ticker(ticker).history(period="5y")
            
            if data.empty:
                failed_tickers.append((ticker, "データが空です"))
                continue
                
            df_t = data.reset_index()
            df_t['Ticker'] = ticker
            
            # 各戦略で使用する移動平均線(SMA)を事前計算
            # これによりスクリーナー側の KeyError: 'SMA200' を防止します
            df_t['SMA25'] = df_t['Close'].rolling(window=25).mean()
            df_t['SMA75'] = df_t['Close'].rolling(window=75).mean()
            df_t['SMA200'] = df_t['Close'].rolling(window=200).mean()
            
            # BigQuery書き込み用に日付型を調整し、タイムゾーンを削除
            df_t['Date'] = pd.to_datetime(df_t['Date']).dt.tz_localize(None)
            
            # 必要なカラムに絞り込み
            cols = ['Date', 'Ticker', 'Open', 'High', 'Low', 'Close', 'Volume', 'SMA25', 'SMA75', 'SMA200']
            all_dfs.append(df_t[cols])
            
            # アクセス制限回避のためのウェイト
            time.sleep(0.1)
            
            if (i + 1) % 100 == 0:
                print(f" [{i+1}/{len(target_tickers)}] 銘柄取得完了...")
                
        except Exception as e:
            failed_tickers.append((ticker, str(e)))

    # 全データの結合
    if all_dfs:
        df_to_upload = pd.concat(all_dfs, ignore_index=True)
        # 全ての列を文字列に変換（BigQueryの互換性確保）
        df_to_upload.columns = df_to_upload.columns.astype(str)
        
        print(f"\nBigQueryへデータをアップロード中... (総件数: {len(df_to_upload):,}行)")
        try:
            # if_exists='replace' を使うことで、古い分割前データを完全に消去し、
            # 最新の「調整済みデータ」でデータベースを再構築します
            pandas_gbq.to_gbq(
                df_to_upload,
                f"{DATASET_ID}.{TABLE_ID}",
                project_id=PROJECT_ID,
                credentials=creds,
                if_exists='replace'
            )
            print("✅ アップロード成功！(全件洗い替え完了)")
        except Exception as e:
            print(f"❌ アップロード失敗: {e}")
    else:
        print("\n⚠️ アップロードするデータがありませんでした。")

    # 失敗レポート
    if failed_tickers:
        print(f"\n[取得失敗レポート: {len(failed_tickers)}件]")
        for t, reason in failed_tickers[:10]:
            print(f"  - {t:<6s} : {reason}")
    else:
        print("\n ✅ 全銘柄のデータ取得に成功しました！")

if __name__ == "__main__":
    main()
