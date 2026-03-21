import pandas as pd
LOOKBACK_DAYS = 375

class CupWithHandleScreener:
    drop_reasons = {
        f"01_データ不足({LOOKBACK_DAYS}日)": 0, "02_基本流動性不足(1ヶ月最低10万株)": 0,
        "03_事前の長期上昇トレンドがない(200日線未満または下向き)": 0, "04_最高値が近すぎる(カップの期間不足)": 0,
        "05_カップの深さが規格外(調整が10%〜45%ではない)": 0, "06_右側の回復が不十分(元の高値付近まで戻っていない)": 0,
        "07_取っ手(ハンドル)を形成する期間がない": 0, "08_ハンドルの位置が低い(カップの下半分まで落ちている)": 0,
        "09_取っ手での売り枯れがない(出来高が平均以上)": 0, "10_取っ手のボラティリティが高すぎる(振れ幅10%超)": 0,
        "11_現在価格が最高値から離れている(-5%〜0%の範囲外)": 0
    }
    @classmethod
    def reset_reasons(cls):
        for key in cls.drop_reasons: cls.drop_reasons[key] = 0
    @classmethod
    def check_conditions(cls, df):
        if len(df) < LOOKBACK_DAYS:
            cls.drop_reasons[f"01_データ不足({LOOKBACK_DAYS}日)"] += 1; return False
        latest = df.iloc[-1]
        if latest['Vol_1M_min'] < 100000:
            cls.drop_reasons["02_基本流動性不足(1ヶ月最低10万株)"] += 1; return False
        if pd.isna(latest['MA200']) or latest['Close'] < latest['MA200'] or latest['MA200'] <= latest['MA200_20d_ago']:
            cls.drop_reasons["03_事前の長期上昇トレンドがない(200日線未満または下向き)"] += 1; return False
        df_lookback = df.iloc[-LOOKBACK_DAYS:]
        high_val = df_lookback['High'].max()
        high_idx = df_lookback['High'].values.argmax()
        if ((LOOKBACK_DAYS - 1) - high_idx) < 20:
            cls.drop_reasons["04_最高値が近すぎる(カップの期間不足)"] += 1; return False
        cup_data = df_lookback.iloc[high_idx:]
        cup_low = cup_data['Low'].min()
        if not (-0.45 <= ((cup_low / high_val) - 1) <= -0.10):
            cls.drop_reasons["05_カップの深さが規格外(調整が10%〜45%ではない)"] += 1; return False
        cup_low_idx = cup_data['Low'].values.argmin()
        right_side_data = cup_data.iloc[cup_low_idx:]
        right_edge_high = right_side_data['High'].max()
        if right_edge_high < high_val * 0.90:
            cls.drop_reasons["06_右側の回復が不十分(元の高値付近まで戻っていない)"] += 1; return False
        handle_data = right_side_data.iloc[right_side_data['High'].values.argmax():]
        if len(handle_data) < 3:
            cls.drop_reasons["07_取っ手(ハンドル)を形成する期間がない"] += 1; return False
        handle_low = handle_data['Low'].min()
        if handle_low < (high_val - (high_val - cup_low) / 2):
            cls.drop_reasons["08_ハンドルの位置が低い(カップの下半分まで落ちている)"] += 1; return False
        if handle_data['Volume'].mean() >= latest['Vol_1M']:
            cls.drop_reasons["09_取っ手での売り枯れがない(出来高が平均以上)"] += 1; return False
        if ((handle_low / right_edge_high) - 1) < -0.10:
            cls.drop_reasons["10_取っ手のボラティリティが高すぎる(振れ幅10%超)"] += 1; return False
        if not (-0.05 <= ((latest['Close'] / high_val) - 1) <= 0.00):
            cls.drop_reasons["11_現在価格が最高値から離れている(-5%〜0%の範囲外)"] += 1; return False
        return True
