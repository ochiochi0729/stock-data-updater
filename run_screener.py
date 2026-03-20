import os
import sys
import json
import pandas as pd
import pandas_gbq
from google.oauth2 import service_account

# ==========================================
# 設定部分
# ==========================================
PROJECT_ID = 'stock-data-updater-490714'  # ★設定済み
DATASET_ID = 'stock_db'
VIEW_ID = 'clean_daily_prices'
CSV_LIST_PATH = 'tickers_list.csv'

# ★ 毎日の自動実行用は None（過去検証時は '2025-06-03' などを指定）
TARGET_DATE = '2025-6-3'

EVAL_DAYS = [3, 6, 7, 10, 13, 16, 19, 22, 25, 28]
STOP_LOSS_THRESHOLD = -0.05
BENCHMARK_TICKER = '1306.T'

# CwH用設定
LOOKBACK_DAYS = 375

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

sys.stdout = Logger("report.txt")

# ==========================================
# 統合：指標計算クラス
# ==========================================
class IndicatorCalculator:
    @staticmethod
    def add_indicators(df):
        # PO（パーフェクトオーダー）用
        df['MA25'] = df['Close'].rolling(window=25).mean()
        df['MA75'] = df['Close'].rolling(window=75).mean()
        df['MA25_5d_ago'] = df['MA25'].shift(5)
        df['MA75_5d_ago'] = df['MA75'].shift(5)
        df['Avg_Range'] = (df['High'] - df['Low']).rolling(window=20).mean()
        
        # CwH（カップ・ウィズ・ハンドル）用
        df['MA200'] = df['Close'].rolling(window=200).mean()
        df['MA200_20d_ago'] = df['MA200'].shift(20)
        
        # Breakout（ブレイクアウト）用追加
        df['MA25_3d_ago'] = df['MA25'].shift(3)
        df['MA75_10d_ago'] = df['MA75'].shift(10)
        df['Close_1d_ago'] = df['Close'].shift(1)
        
        # 共通
        df['Vol_1M'] = df['Volume'].rolling(window=20).mean()
        df['Vol_1M_min'] = df['Volume'].rolling(window=20).min()
        return df

# ==========================================
# 検査A：パーフェクトオーダー判定
# ==========================================
class PerfectOrderScreener:
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
        for key in cls.drop_reasons: cls.drop_reasons[key] = 0
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

# ==========================================
# 検査B：カップ・ウィズ・ハンドル判定
# ==========================================
class CupWithHandleScreener:
    drop_reasons = {
        f"01_データ不足({LOOKBACK_DAYS}日)": 0,
        "02_基本流動性不足(1ヶ月最低10万株)": 0,
        "03_事前の長期上昇トレンドがない(200日線未満または下向き)": 0,
        "04_最高値が近すぎる(カップの期間不足)": 0,
        "05_カップの深さが規格外(調整が10%〜45%ではない)": 0,
        "06_右側の回復が不十分(元の高値付近まで戻っていない)": 0,
        "07_取っ手(ハンドル)を形成する期間がない": 0,
        "08_ハンドルの位置が低い(カップの下半分まで落ちている)": 0,
        "09_取っ手での売り枯れがない(出来高が平均以上)": 0,
        "10_取っ手のボラティリティが高すぎる(振れ幅10%超)": 0,
        "11_現在価格が最高値から離れている(-5%〜0%の範囲外)": 0
    }
    @classmethod
    def reset_reasons(cls):
        for key in cls.drop_reasons: cls.drop_reasons[key] = 0
    @classmethod
    def check_conditions(cls, df):
        if len(df) < LOOKBACK_DAYS:
            cls.drop_reasons[f"01_データ不足({LOOKBACK_DAYS}日)"] += 1
            return False
        latest = df.iloc[-1]
        if latest['Vol_1M_min'] < 100000:
            cls.drop_reasons["02_基本流動性不足(1ヶ月最低10万株)"] += 1
            return False
        if pd.isna(latest['MA200']) or latest['Close'] < latest['MA200'] or latest['MA200'] <= latest['MA200_20d_ago']:
            cls.drop_reasons["03_事前の長期上昇トレンドがない(200日線未満または下向き)"] += 1
            return False
        df_lookback = df.iloc[-LOOKBACK_DAYS:]
        high_val = df_lookback['High'].max()
        high_idx = df_lookback['High'].values.argmax()
        days_since_high = (LOOKBACK_DAYS - 1) - high_idx
        if days_since_high < 20:
            cls.drop_reasons["04_最高値が近すぎる(カップの期間不足)"] += 1
            return False
        cup_data = df_lookback.iloc[high_idx:]
        cup_low = cup_data['Low'].min()
        cup_depth = (cup_low / high_val) - 1
        if not (-0.45 <= cup_depth <= -0.10):
            cls.drop_reasons["05_カップの深さが規格外(調整が10%〜45%ではない)"] += 1
            return False
        cup_low_idx = cup_data['Low'].values.argmin()
        right_side_data = cup_data.iloc[cup_low_idx:]
        right_edge_high = right_side_data['High'].max()
        if right_edge_high < high_val * 0.90:
            cls.drop_reasons["06_右側の回復が不十分(元の高値付近まで戻っていない)"] += 1
            return False
        right_edge_idx = right_side_data['High'].values.argmax()
        handle_data = right_side_data.iloc[right_edge_idx:]
        if len(handle_data) < 3:
            cls.drop_reasons["07_取っ手(ハンドル)を形成する期間がない"] += 1
            return False
        handle_low = handle_data['Low'].min()
        cup_midpoint = high_val - (high_val - cup_low) / 2
        if handle_low < cup_midpoint:
            cls.drop_reasons["08_ハンドルの位置が低い(カップの下半分まで落ちている)"] += 1
            return False
        handle_vol_avg = handle_data['Volume'].mean()
        if handle_vol_avg >= latest['Vol_1M']:
            cls.drop_reasons["09_取っ手での売り枯れがない(出来高が平均以上)"] += 1
            return False
        handle_drop = (handle_low / right_edge_high) - 1
        if handle_drop < -0.10:
            cls.drop_reasons["10_取っ手のボラティリティが高すぎる(振れ幅10%超)"] += 1
            return False
        current_vs_high = (latest['Close'] / high_val) - 1
        if not (-0.05 <= current_vs_high <= 0.00):
            cls.drop_reasons["11_現在価格が最高値から離れている(-5%〜0%の範囲外)"] += 1
            return False
        return True

# ==========================================
# 検査C：ブレイクアウト判定（NEW!!）
# ==========================================
class BreakoutScreener:
    drop_reasons = {
        "1_データ不足": 0,
        "2_基本流動性不足": 0,
        "3a_75日線が過去に下降トレンドではない(上昇中など)": 0,
        "3b_75日線の直近の下降が落ち着いていない(±1.5%の範囲外)": 0, 
        "3c_25日線と75日線が離れすぎている(底練り未完了)": 0,
        "4_25日線が下向き": 0,
        "5_直近で下から上抜けしていない": 0,
        "6_価格が25日線の範囲外(-1%〜+5%)": 0, 
        "7_直近5日間の上昇＆出来高急増なし": 0, 
        "8_価格が75日線の下にある(上値抵抗線のダマシ回避)": 0
    }
    @classmethod
    def reset_reasons(cls):
        for key in cls.drop_reasons: cls.drop_reasons[key] = 0
    @classmethod
    def check_conditions(cls, df):
        if len(df) < 80:
            cls.drop_reasons["1_データ不足"] += 1
            return False
        latest = df.iloc[-1]
        if latest['Vol_1M_min'] < 100000:
            cls.drop_reasons["2_基本流動性不足"] += 1
            return False
        ma75_40d_ago = df['MA75'].iloc[-40]
        ma75_10d_ago = latest['MA75_10d_ago']
        if ma75_40d_ago <= ma75_10d_ago:
            cls.drop_reasons["3a_75日線が過去に下降トレンドではない(上昇中など)"] += 1
            return False
        ma75_recent_change = (latest['MA75'] / ma75_10d_ago) - 1
        if not (-0.015 <= ma75_recent_change <= 0.015):
            cls.drop_reasons["3b_75日線の直近の下降が落ち着いていない(±1.5%の範囲外)"] += 1
            return False
        ma_distance = abs(latest['MA25'] - latest['MA75']) / latest['MA75']
        if ma_distance > 0.05:
            cls.drop_reasons["3c_25日線と75日線が離れすぎている(底練り未完了)"] += 1
            return False
        ma25_change_rate = (latest['MA25'] / latest['MA25_3d_ago']) - 1
        if ma25_change_rate < -0.001:
            cls.drop_reasons["4_25日線が下向き"] += 1
            return False
        was_below_ma25 = (df['Close'].iloc[-6:-1] < df['MA25'].iloc[-6:-1]).any()
        if not was_below_ma25:
            cls.drop_reasons["5_直近で下から上抜けしていない"] += 1
            return False
        price_to_ma25 = latest['Close'] / latest['MA25']
        if not (0.99 <= price_to_ma25 <= 1.05):
            cls.drop_reasons["6_価格が25日線の範囲外(-1%〜+5%)"] += 1
            return False
        has_volume_breakout = False
        for i in range(1, 6):
            idx = -i
            vol = df['Volume'].iloc[idx]
            vol_ma = df['Vol_1M'].iloc[idx]
            close_price = df['Close'].iloc[idx]
            open_price = df['Open'].iloc[idx]
            prev_close = df['Close_1d_ago'].iloc[idx]
            if (vol >= vol_ma * 1.5) and ((close_price > open_price) or (close_price > prev_close)):
                has_volume_breakout = True
                break
        if not has_volume_breakout:
            cls.drop_reasons["7_直近5日間の上昇＆出来高急増なし"] += 1
            return False
        if latest['Close'] <= latest['MA75']:
            cls.drop_reasons["8_価格が75日線の下にある(上値抵抗線のダマシ回避)"] += 1
            return False
        return True

# ==========================================
# メイン処理・実行ヘルパー
# ==========================================
def get_credentials():
    creds_json = os.environ.get('GCP_CREDENTIALS')
    if not creds_json: raise ValueError("合鍵が見つかりません。")
    return service_account.Credentials.from_service_account_info(json.loads(creds_json))

def load_tickers_from_csv(file_path):
    if not os.path.exists(file_path): return []
    try:
        df = pd.read_csv(file_path)
        raw_codes = df.iloc[:, 0].astype(str)
        return [c.strip().upper() + '.T' if not c.strip().upper().endswith('.T') else c.strip().upper() for c in raw_codes if c.strip()]
    except: return []

def run_screening_loop(screener_class, strategy_name, target_tickers, dict_dfs, actual_eval_date):
    print(f"\n{'='*80}")
    print(f"▼ スクリーニング実行: {strategy_name}")
    print(f"{'='*80}")
    
    screener_class.reset_reasons()
    hit_tickers = []
    aggregate_returns = {d: [] for d in EVAL_DAYS}
    total_tickers = len(target_tickers)

    for i, ticker in enumerate(target_tickers):
        if (i + 1) % 500 == 0: print(f" ... スキャン進行度: {i + 1} / {total_tickers} 銘柄完了")
        if ticker not in dict_dfs: continue
        
        df_full = dict_dfs[ticker]
        df = df_full.copy()

        if TARGET_DATE:
            try: df = df.loc[:TARGET_DATE].copy()
            except KeyError: continue
            
        if len(df) == 0: continue

        is_hit = screener_class.check_conditions(df)
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

            row_1_strs, row_2_strs = [], []
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
                if idx < 5: row_1_strs.append(display_str)
                else: row_2_strs.append(display_str)

            print(f"\n★ 抽出: {ticker:<6s} | 基準日終値: {hit_price:,.1f}円 -> 翌日始値: {entry_str}")
            print(f"   ├ " + " | ".join(row_1_strs))
            print(f"   └ " + " | ".join(row_2_strs))
            print("-" * 80)

    print(f"\n{strategy_name} 抽出完了: {len(hit_tickers)}銘柄")
    if hit_tickers:
        print(f"合致銘柄: {', '.join(hit_tickers)}")
        
        print(f"\n [抽出銘柄 平均]")
        avg_1_strs, avg_2_strs = [], []
        for idx, d in enumerate(EVAL_DAYS):
            if aggregate_returns[d]:
                avg_perf = sum(aggregate_returns[d]) / len(aggregate_returns[d])
                avg_str = f"{d:>2}d: {avg_perf:>+5.1%} ({len(aggregate_returns[d])}銘柄)"
            else:
                avg_str = f"{d:>2}d: データなし"
            if idx < 5: avg_1_strs.append(avg_str)
            else: avg_2_strs.append(avg_str)
        print(f" ├ " + " | ".join(avg_1_strs))
        print(f" └ " + " | ".join(avg_2_strs))

    print("\n[脱落理由のレポート]")
    for reason, count in sorted(screener_class.drop_reasons.items()):
        print(f"{reason}: {count} 銘柄")


# ==========================================
# メイン実行ブロック
# ==========================================
if __name__ == "__main__":
    target_tickers = load_tickers_from_csv(CSV_LIST_PATH)
    if BENCHMARK_TICKER in target_tickers: target_tickers.remove(BENCHMARK_TICKER)

    if not target_tickers:
        print("銘柄リストが見つかりません。")
        exit()

    print("BigQueryから必要な株価データを一括ダウンロード中...（魔法の鏡を読み込みます）")
    try:
        # CwHのために600日（約2年強）分を遡ってダウンロード（Breakoutもこれで十分カバー可能）
        if TARGET_DATE:
            target_dt = pd.to_datetime(TARGET_DATE)
            start_date_str = (target_dt - pd.Timedelta(days=600)).strftime('%Y-%m-%d')
            end_date_str = (target_dt + pd.Timedelta(days=45)).strftime('%Y-%m-%d')
            query = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.{VIEW_ID}` WHERE Date >= '{start_date_str}' AND Date <= '{end_date_str}'"
        else:
            query = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.{VIEW_ID}` WHERE Date >= DATE_SUB(CURRENT_DATE('Asia/Tokyo'), INTERVAL 600 DAY)"
            
        df_all = pandas_gbq.read_gbq(query, project_id=PROJECT_ID, credentials=get_credentials())
        df_all['Date'] = pd.to_datetime(df_all['Date'])
        
        # ダウンロード直後に全銘柄の指標を一括計算しておく
        dict_dfs = {}
        for ticker, group in df_all.groupby('Ticker'):
            df = group.set_index('Date').sort_index()
            dict_dfs[ticker] = IndicatorCalculator.add_indicators(df)
            
    except Exception as e:
        print(f"BigQueryの読み込みエラー: {e}")
        exit()

    date_str = TARGET_DATE if TARGET_DATE else "最新(本日)"
    print(f"\n基準日【{date_str}】での統合スクリーニングを開始します...\n")

    # 実際の評価日（ETF同期用）を取得
    actual_eval_date = None
    sample_df = next(iter(dict_dfs.values()))
    if TARGET_DATE:
        sample_df = sample_df.loc[:TARGET_DATE]
    if len(sample_df) > 0:
        actual_eval_date = sample_df.index[-1]

    # --- 検査A（パーフェクトオーダー）を実行 ---
    run_screening_loop(PerfectOrderScreener, "①パーフェクトオーダー押し目買い", target_tickers, dict_dfs, actual_eval_date)

    # --- 検査B（カップ・ウィズ・ハンドル）を実行 ---
    run_screening_loop(CupWithHandleScreener, "②カップ・ウィズ・ハンドル", target_tickers, dict_dfs, actual_eval_date)
    
    # --- 検査C（ブレイクアウト）を実行 ---
    run_screening_loop(BreakoutScreener, "③底練りからのブレイクアウト", target_tickers, dict_dfs, actual_eval_date)
    
    print("\n" + "="*80)
    print("すべてのスクリーニングが完了しました。")
    print("="*80)
