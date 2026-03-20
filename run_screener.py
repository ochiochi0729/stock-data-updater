import os
import sys
import json
import pandas as pd
import pandas_gbq
from google.oauth2 import service_account

# --- 設定部分 ---
PROJECT_ID = 'stock-data-updater-490714'  # ★変更必須
DATASET_ID = 'stock_db'
VIEW_ID = 'clean_daily_prices'
CSV_LIST_PATH = 'tickers_list.csv'

# ★ 毎日最新データで実行するため None に設定
TARGET_DATE = '2025-6-3'

EVAL_DAYS = [3, 6, 7, 10, 13, 16, 19, 22, 25, 28]
STOP_LOSS_THRESHOLD = -0.05
BENCHMARK_TICKER = '1306.T'

# ==============================================================
# 出力結果をメール送信用にテキストファイルに保存する魔法のクラス
# ==============================================================
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

sys.stdout = Logger("report.txt")

# ==============================================================
# 指標計算・判定ロジック（※一切変更していません）
# ==============================================================
class IndicatorCalculator:
    """指標計算クラス"""
    @staticmethod
    def add_indicators(df):
        df['MA25'] = df['Close'].rolling(window=25).mean()
        df['MA75'] = df['Close'].rolling(window=75).mean()
        df['MA25_5d_ago'] = df['MA25'].shift(5)
        df['MA75_5d_ago'] = df['MA75'].shift(5)
        df['Vol_1M'] = df['Volume'].rolling(window=20).mean()
        df['Vol_1M_min'] = df['Volume'].rolling(window=20).min()
        df['Avg_Range'] = (df['High'] - df['Low']).rolling(window=20).mean()
        return df

class StockScreener:
    """パーフェクトオーダー押し目判定クラス"""
    drop_reasons = {
        "1_データ不足(100日)": 0,
        "2_基本流動性不足(1ヶ月最低10万株)": 0,
        "3_パーフェクトオーダーの並びではない": 0,
        "4_移動平均線が上向きではない(トレンド弱)": 0,
        "5a_数日前に十分な上昇(MA25の+3%以上)がない": 0,
        "5b_直近高値からの下落が大きすぎる(10%以上の暴落)": 0,
        "6_現在価格が25日線の押し目範囲外(0%〜2%)": 0,
        "7_直近の出来高が細っていない(売り枯れ未完了)": 0,
        "8_反発の事実がない(前日比プラスの陽線等がない)": 0,
        "9_異常なボラティリティ(決算等のノイズ回避)": 0
    }
    @classmethod
    def reset_reasons(cls):
        for key in cls.drop_reasons.keys():
            cls.drop_reasons[key] = 0
    @classmethod
    def check_conditions(cls, df):
        if len(df) < 100:
            cls.drop_reasons["1_データ不足(100日)"] += 1
            return False
        latest = df.iloc[-1]
        prev = df.iloc[-2]
        if latest['Vol_1M_min'] < 100000:
            cls.drop_reasons["2_基本流動性不足(1ヶ月最低10万株)"] += 1
            return False
        if latest['MA25'] <= latest['MA75']:
            cls.drop_reasons["3_パーフェクトオーダーの並びではない"] += 1
            return False
        if (latest['MA25'] <= latest['MA25_5d_ago']) or (latest['MA75'] <= latest['MA75_5d_ago']):
            cls.drop_reasons["4_移動平均線が上向きではない(トレンド弱)"] += 1
            return False
        highs_recent = df['High'].iloc[-15:-2]
        ma25_recent = df['MA25'].iloc[-15:-2]
        if not (highs_recent > ma25_recent * 1.03).any():
            cls.drop_reasons["5a_数日前に十分な上昇(MA25の+3%以上)がない"] += 1
            return False
        recent_max_high = highs_recent.max()
        drop_rate = (latest['Low'] / recent_max_high) - 1
        if drop_rate < -0.10:
            cls.drop_reasons["5b_直近高値からの下落が大きすぎる(10%以上の暴落)"] += 1
            return False
        current_vs_ma25 = (latest['Close'] / latest['MA25']) - 1
        if not (-0.00 <= current_vs_ma25 <= 0.02):
            cls.drop_reasons["6_現在価格が25日線の押し目範囲外(0%〜2%)"] += 1
            return False
        vol_3d_avg = df['Volume'].iloc[-3:].mean()
        if vol_3d_avg >= latest['Vol_1M']:
            cls.drop_reasons["7_直近の出来高が細っていない(売り枯れ未完了)"] += 1
            return False
        is_yang = latest['Close'] > latest['Open']
        higher_close = latest['Close'] > prev['Close']
        break_high = latest['Close'] > prev['High']
        if not (break_high or (is_yang and higher_close)):
            cls.drop_reasons["8_反発の事実がない(前日比プラスの陽線等がない)"] += 1
            return False
        today_range = latest['High'] - latest['Low']
        avg_range = prev['Avg_Range']
        gap = abs((latest['Open'] / prev['Close']) - 1)
        if (today_range > avg_range * 2.5) or (gap > 0.04):
            cls.drop_reasons["9_異常なボラティリティ(決算等のノイズ回避)"] += 1
            return False
        return True
    @classmethod
    def print_report(cls):
        print("\n=== 脱落理由のレポート ===")
        for reason, count in cls.drop_reasons.items():
            print(f"{reason}: {count} 銘柄")

# ==============================================================
# メイン処理（BigQuery一括読み込み対応）
# ==============================================================
def get_credentials():
    creds_json = os.environ.get('GCP_CREDENTIALS')
    if not creds_json:
        raise ValueError("合鍵が見つかりません。")
    creds_dict = json.loads(creds_json)
    return service_account.Credentials.from_service_account_info(creds_dict)

def load_tickers_from_csv(file_path):
    if not os.path.exists(file_path):
        return []
    try:
        df = pd.read_csv(file_path)
        raw_codes = df.iloc[:, 0].astype(str)
        return [c.strip().upper() + '.T' if not c.strip().upper().endswith('.T') else c.strip().upper() for c in raw_codes if c.strip()]
    except:
        return []

if __name__ == "__main__":
    target_tickers = load_tickers_from_csv(CSV_LIST_PATH)
    hit_tickers = []
    StockScreener.reset_reasons()
    aggregate_returns = {d: [] for d in EVAL_DAYS}
    actual_eval_date = None

    if BENCHMARK_TICKER in target_tickers:
        target_tickers.remove(BENCHMARK_TICKER)

    if not target_tickers:
        print("銘柄リストが見つかりません。")
        exit()

    print("BigQueryから必要な株価データを一括ダウンロード中...（魔法の鏡を読み込みます）")
    try:
        # ★ 過去検証（TARGET_DATE）に対応する賢いSQLの組み立て
        if TARGET_DATE:
            target_dt = pd.to_datetime(TARGET_DATE)
            start_date_str = (target_dt - pd.Timedelta(days=200)).strftime('%Y-%m-%d')
            # ★ 追加：答え合わせ用に「未来45日分」も余分に取得する
            end_date_str = (target_dt + pd.Timedelta(days=45)).strftime('%Y-%m-%d')
            
            query = f"""
                SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.{VIEW_ID}`
                WHERE Date >= '{start_date_str}' AND Date <= '{end_date_str}'
            """
        else:
            # 毎日の自動実行（最新）の場合、今日から遡って200日分を取得
            query = f"""
                SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.{VIEW_ID}`
                WHERE Date >= DATE_SUB(CURRENT_DATE('Asia/Tokyo'), INTERVAL 200 DAY)
            """
            
        df_all = pandas_gbq.read_gbq(query, project_id=PROJECT_ID, credentials=get_credentials())
        df_all['Date'] = pd.to_datetime(df_all['Date'])
        dict_dfs = {ticker: group.set_index('Date').sort_index() for ticker, group in df_all.groupby('Ticker')}
    except Exception as e:
        print(f"BigQueryの読み込みエラー: {e}")
        exit()

    date_str = TARGET_DATE if TARGET_DATE else "最新(本日)"
    print(f"\n基準日【{date_str}】でのパーフェクトオーダー押し目買い スクリーニングを開始します...\n")

    total_tickers = len(target_tickers)

    for i, ticker in enumerate(target_tickers):
        if (i + 1) % 100 == 0:
            print(f" ... スキャン進行度: {i + 1} / {total_tickers} 銘柄完了")

        if ticker in dict_dfs:
            df_full = dict_dfs[ticker].copy()
            df = df_full.copy()

            if TARGET_DATE:
                try:
                    df = df.loc[:TARGET_DATE].copy()
                except KeyError:
                    continue

            if actual_eval_date is None and len(df) > 0:
                actual_eval_date = df.index[-1]

            if len(df) < 100:
                StockScreener.drop_reasons["1_データ不足(100日)"] += 1
                continue

            df_with_indicators = IndicatorCalculator.add_indicators(df)
            is_hit = StockScreener.check_conditions(df_with_indicators)

            if is_hit:
                hit_tickers.append(ticker)
                hit_price = df.iloc[-1]['Close']
                current_idx = len(df) - 1
                max_idx = len(df_full) - 1

                entry_price = None
                entry_str = "データなし(未来)"
                if current_idx + 1 <= max_idx:
                    entry_price = df_full.iloc[current_idx + 1]['Open']
                    entry_str = f"{entry_price:,.1f}円"

                row_1_strs = []
                row_2_strs = []
                is_stopped_out = False

                for idx, d in enumerate(EVAL_DAYS):
                    target_idx = current_idx + d
                    if is_stopped_out:
                        perf_str = "損切除外"
                    elif target_idx <= max_idx and entry_price is not None:
                        future_price = df_full.iloc[target_idx]['Close']
                        perf = (future_price / entry_price) - 1
                        if perf <= STOP_LOSS_THRESHOLD:
                            is_stopped_out = True
                            perf_str = f"{future_price:,.0f}円({perf:>+5.1%} 損切)"
                        else:
                            aggregate_returns[d].append(perf)
                            perf_str = f"{future_price:,.0f}円({perf:>+5.1%})"
                    else:
                        perf_str = "-"

                    display_str = f"{d:>2}日後: {perf_str}"
                    if idx < 5:
                        row_1_strs.append(display_str)
                    else:
                        row_2_strs.append(display_str)

                print("\n" + f"★ 抽出: {ticker:<6s} | 基準日終値: {hit_price:,.1f}円 -> 翌日始値: {entry_str}")
                print(f"   ├ " + " | ".join(row_1_strs))
                print(f"   └ " + " | ".join(row_2_strs))
                print("-" * 100)

    print(f"\n\nスクリーニング完了。抽出された銘柄数: {len(hit_tickers)}")
    if hit_tickers:
        print(f"合致銘柄: {', '.join(hit_tickers)}")

        topix_returns = {d: None for d in EVAL_DAYS}
        if actual_eval_date is not None and BENCHMARK_TICKER in dict_dfs:
            bench_full = dict_dfs[BENCHMARK_TICKER]
            if actual_eval_date in bench_full.index:
                bench_idx = bench_full.index.get_loc(actual_eval_date)
                max_bench_idx = len(bench_full) - 1
                bench_entry_price = None
                if bench_idx + 1 <= max_bench_idx:
                    bench_entry_price = bench_full.iloc[bench_idx + 1]['Open']
                for d in EVAL_DAYS:
                    target_idx = bench_idx + d
                    if bench_entry_price is not None and target_idx <= max_bench_idx:
                        bench_close = bench_full.iloc[target_idx]['Close']
                        topix_returns[d] = (bench_close / bench_entry_price) - 1
        else:
            print(f"\n※ 注意: 比較用のベンチマーク ({BENCHMARK_TICKER}) のデータがないため計算されません。")

        print("\n" + "=" * 100)
        print(f"【全体サマリー】 翌日始値(寄り付き)エントリーの平均変動率推移 (損切ライン: {STOP_LOSS_THRESHOLD:.0%})")
        print("=" * 100)
        print(" [抽出銘柄 平均]")
        avg_1_strs, avg_2_strs = [], []
        for idx, d in enumerate(EVAL_DAYS):
            if aggregate_returns[d]:
                avg_perf = sum(aggregate_returns[d]) / len(aggregate_returns[d])
                survivors = len(aggregate_returns[d])
                avg_str = f"{d:>2}日後: {avg_perf:>+5.1%} ({survivors}銘柄)"
            else:
                avg_str = f"{d:>2}日後: データなし"
            if idx < 5: avg_1_strs.append(avg_str)
            else: avg_2_strs.append(avg_str)

        print(f" ├ " + " | ".join(avg_1_strs))
        print(f" └ " + " | ".join(avg_2_strs))
        print("-" * 100)

        print(f" [参考: TOPIX連動ETF ({BENCHMARK_TICKER}) の実績]")
        topix_1_strs, topix_2_strs = [], []
        for idx, d in enumerate(EVAL_DAYS):
            if d in topix_returns and topix_returns[d] is not None:
                t_str = f"{d:>2}日後: {topix_returns[d]:>+5.1%}      "
            else:
                t_str = f"{d:>2}日後: データなし"
            if idx < 5: topix_1_strs.append(t_str)
            else: topix_2_strs.append(t_str)

        print(f" ├ " + " | ".join(topix_1_strs))
        print(f" └ " + " | ".join(topix_2_strs))
        print("=" * 100)

    StockScreener.print_report()
