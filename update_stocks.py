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
    if not creds_json: raise ValueError("GCPの認証情報(GCP_CREDENTIALS)が見つかりません。")
    return service_account.Credentials.from_service_account_info(json.loads(creds_json))

def load_tickers_from_csv():
    if not os.path.exists(CSV_LIST_PATH): return []
    try:
        df = pd.read_csv(CSV_LIST_PATH, header=None)
        return [c.strip().upper() + '.T' if not c.strip().upper().endswith('.T') else c.strip().upper() for c in df.iloc[:, 0].astype(str) if c.strip()]
    except: return []

# ★ 追加ポイント①：CSVから「銘柄コードと銘柄名の辞書（マップ）」を作る関数
def get_brand_map():
    if not os.path.exists(CSV_LIST_PATH): return {}
    try:
        df = pd.read_csv(CSV_LIST_PATH, header=None)
        df[0] = df[0].astype(str).str.strip().str.upper()
        df[0] = df[0].apply(lambda x: x + '.T' if not x.endswith('.T') else x)
        
        # B列(列番号1)が存在する場合のみ辞書化。空欄ならコードを名前にする
        if df.shape[1] > 1:
            df[1] = df[1].fillna(df[0]).astype(str)
            return dict(zip(df[0], df[1]))
        else:
            return dict(zip(df[0], df[0]))
    except:
        return {}

# ==========================================
# メイン処理
# ==========================================
def main():
    print(f"{'='*60}\n▼ 株価データベース更新レポート (株式分割対応・全件洗い替え)\n{'='*60}")
    
    target_tickers = load_tickers_from_csv()
    if not target_tickers:
        print("エラー: 銘柄リストが見つかりません。")
        return

    # ★ 追加ポイント②：作成した関数を使って、銘柄名辞書を読み込む
    brand_map = get_brand_map()

    creds = get_credentials()
    
    BENCHMARK_TICKER = "1306.T"
    if BENCHMARK_TICKER not in target_tickers:
        target_tickers.append(BENCHMARK_TICKER)
        # ベンチマークの名前も登録しておく
        brand_map[BENCHMARK_TICKER] = "TOPIX連動ETF"

    print(f"対象銘柄数: {len(target_tickers)}件")
    print(f"取得モード: 過去5年分を全件取得してBigQueryを上書き(replace)\n")

    failed_tickers = []
    all_dfs = []  

    for i, ticker in enumerate(target_tickers):
        try:
            data = yf.Ticker(ticker).history(period="5y")
            
            if data.empty:
                failed_tickers.append((ticker, "データが空です"))
                continue
                
            df_t = data.reset_index()
            df_t['Ticker'] = ticker
            
            # ★ 追加ポイント③：取得した株価データに BrandName 列を追加する
            # （辞書に見つからなければ、とりあえず銘柄コードを入れておく安全設計）
            df_t['BrandName'] = brand_map.get(ticker, ticker)
            
            df_t['SMA25'] = df_t['Close'].rolling(window=25).mean()
            df_t['SMA75'] = df_t['Close'].rolling(window=75).mean()
            df_t['SMA200'] = df_t['Close'].rolling(window=200).mean()
            
            df_t['Date'] = pd.to_datetime(df_t['Date']).dt.tz_localize(None)
            
            # ★ 追加ポイント④：BigQueryに送る列のリストに 'BrandName' を加える
            cols = ['Date', 'Ticker', 'BrandName', 'Open', 'High', 'Low', 'Close', 'Volume', 'SMA25', 'SMA75', 'SMA200']
            all_dfs.append(df_t[cols])
            
            time.sleep(0.1)
            
            if (i + 1) % 100 == 0:
                print(f" [{i+1}/{len(target_tickers)}] 銘柄取得完了...")
                
        except Exception as e:
            failed_tickers.append((ticker, str(e)))

    if all_dfs:
        df_to_upload = pd.concat(all_dfs, ignore_index=True)
        df_to_upload.columns = df_to_upload.columns.astype(str)
        
        print(f"\nBigQueryへデータをアップロード中... (総件数: {len(df_to_upload):,}行)")
        try:
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

    if failed_tickers:
        print(f"\n[取得失敗レポート: {len(failed_tickers)}件]")
        for t, reason in failed_tickers[:10]:
            print(f"  - {t:<6s} : {reason}")
    else:
        print("\n ✅ 全銘柄のデータ取得に成功しました！")

if __name__ == "__main__":
    main()
