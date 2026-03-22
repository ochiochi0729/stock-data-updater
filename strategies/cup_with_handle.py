import pandas as pd
import numpy as np

LOOKBACK_DAYS = 375

class CupWithHandleScreener:
    def __init__(self):
        # インスタンス変数に変更
        self.drop_reasons = {
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

    def reset_reasons(self):
        for key in self.drop_reasons:
            self.drop_reasons[key] = 0

    def check_conditions(self, df):
        # 01. データ不足
        if len(df) < LOOKBACK_DAYS:
            self.drop_reasons[f"01_データ不足({LOOKBACK_DAYS}日)"] += 1
            return False

        # ---------------------------------------------------------
        # DBに無い指標をスクリーニング時に動的に計算
        # ---------------------------------------------------------
        # 1ヶ月(20日)の出来高関連
        vol_1m_min = df['Volume'].rolling(window=20).min().iloc[-1]
        vol_1m_avg = df['Volume'].rolling(window=20).mean().iloc[-1]

        # 200日移動平均線 (DBに無いため終値から計算)
        sma200_series = df['Close'].rolling(window=200).mean()
        sma200_latest = sma200_series.iloc[-1]
        sma200_20d_ago = sma200_series.shift(20).iloc[-1]

        latest = df.iloc[-1]

        # 02. 基本流動性不足
        if vol_1m_min < 100000:
            self.drop_reasons["02_基本流動性不足(1ヶ月最低10万株)"] += 1
            return False

        # 03. 事前の長期上昇トレンドがない
        if pd.isna(sma200_latest) or latest['Close'] < sma200_latest or sma200_latest <= sma200_20d_ago:
            self.drop_reasons["03_事前の長期上昇トレンドがない(200日線未満または下向き)"] += 1
            return False

        # カップの形成期間データを取得
        df_lookback = df.iloc[-LOOKBACK_DAYS:]
        high_val = df_lookback['High'].max()
        high_idx = df_lookback['High'].values.argmax()

        # 04. 最高値が近すぎる (カップの期間不足)
        if ((LOOKBACK_DAYS - 1) - high_idx) < 20:
            self.drop_reasons["04_最高値が近すぎる(カップの期間不足)"] += 1
            return False

        cup_data = df_lookback.iloc[high_idx:]
        cup_low = cup_data['Low'].min()

        # 05. カップの深さが規格外 (ゼロ除算回避)
        if high_val == 0 or not (-0.45 <= ((cup_low / high_val) - 1) <= -0.10):
            self.drop_reasons["05_カップの深さが規格外(調整が10%〜45%ではない)"] += 1
            return False

        cup_low_idx = cup_data['Low'].values.argmin()
        right_side_data = cup_data.iloc[cup_low_idx:]
        right_edge_high = right_side_data['High'].max()

        # 06. 右側の回復が不十分
        if right_edge_high < high_val * 0.90:
            self.drop_reasons["06_右側の回復が不十分(元の高値付近まで戻っていない)"] += 1
            return False

        handle_data = right_side_data.iloc[right_side_data['High'].values.argmax():]

        # 07. 取っ手(ハンドル)を形成する期間がない
        if len(handle_data) < 3:
            self.drop_reasons["07_取っ手(ハンドル)を形成する期間がない"] += 1
            return False

        handle_low = handle_data['Low'].min()

        # 08. ハンドルの位置が低い
        if handle_low < (high_val - (high_val - cup_low) / 2):
            self.drop_reasons["08_ハンドルの位置が低い(カップの下半分まで落ちている)"] += 1
            return False

        # 09. 取っ手での売り枯れがない
        if handle_data['Volume'].mean() >= vol_1m_avg:
            self.drop_reasons["09_取っ手での売り枯れがない(出来高が平均以上)"] += 1
            return False

        # 10. 取っ手のボラティリティが高すぎる (ゼロ除算回避)
        if right_edge_high == 0 or ((handle_low / right_edge_high) - 1) < -0.10:
            self.drop_reasons["10_取っ手のボラティリティが高すぎる(振れ幅10%超)"] += 1
            return False

        # 11. 現在価格が最高値から離れている (ゼロ除算回避)
        if high_val == 0 or not (-0.05 <= ((latest['Close'] / high_val) - 1) <= 0.00):
            self.drop_reasons["11_現在価格が最高値から離れている(-5%〜0%の範囲外)"] += 1
            return False

        return True
