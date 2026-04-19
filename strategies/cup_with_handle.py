import pandas as pd
import numpy as np

LOOKBACK_DAYS = 375

class CupWithHandleScreener:
    def __init__(self):
        self.drop_reasons = {}
        self.reset_reasons()

    def reset_reasons(self):
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
            # ★ 理由11を「ハンドルのブレイクアウト」に変更しました
            "11_ハンドルの高値をブレイクアウトしていない(初動ではない)": 0
        }

    def get_all_signals(self, df):
        if len(df) < LOOKBACK_DAYS:
            return pd.Series(False, index=df.index)

        close, vol, sma200 = df['Close'], df['Volume'], df['SMA200']
        
        vol_1m_avg = vol.rolling(20).mean()
        vol_1m_min = vol.rolling(20).min()

        c2 = vol_1m_min >= 100000
        c3 = (close > sma200) & (sma200 > sma200.shift(20))

        signals = pd.Series(False, index=df.index)
        
        for i in range(LOOKBACK_DAYS, len(df)):
            window = df.iloc[i-249 : i+1] 
            if self._check_logic_pure(window, vol_1m_avg.iloc[i]):
                signals.iloc[i] = True
        
        return signals & c2 & c3

    def _check_logic_pure(self, cup_data, v_avg):
        # ★ 修正: 昨日まで(past_data) と 本日(curr_close) を完全に分離
        past_data = cup_data.iloc[:-1]
        curr_close = cup_data['Close'].iloc[-1]
        
        h_val = past_data['High'].max()
        h_idx = past_data['High'].values.argmax()
        days_since_high = len(past_data) - h_idx
        if days_since_high < 30: return False
        
        # 底の計算（左の高値より右側から探す）
        past_after_high = past_data.iloc[h_idx:]
        c_low = past_after_high['Low'].min()
        c_low_idx = past_after_high['Low'].values.argmin() + h_idx
        
        depth = (h_val - c_low) / h_val
        if not (0.10 <= depth <= 0.45): return False
        
        r_side = past_data.iloc[c_low_idx:]
        r_peak_val = r_side['High'].max()
        if r_peak_val < h_val * 0.90: return False
        
        r_peak_idx = r_side['High'].values.argmax()
        h_data = r_side.iloc[r_peak_idx:]
        if len(h_data) < 3: return False
        
        h_low = h_data['Low'].min()
        if h_low < (h_val - (h_val - c_low) / 2): return False
        if h_data['Volume'].mean() >= v_avg: return False
        if (r_peak_val / h_low - 1) > 0.10: return False
        
        # ★ 修正: 今日の終値が、ハンドルの高値（右のピーク）をブレイクしたか？
        if curr_close <= r_peak_val: return False
        
        return True

    def check_conditions(self, df):
        if len(df) < LOOKBACK_DAYS:
            self.drop_reasons[f"01_データ不足({LOOKBACK_DAYS}日)"] += 1
            return False
            
        sig = self.get_all_signals(df)
        if not sig.empty and sig.iloc[-1]: 
            return True
        
        self._update_drop_reasons(df)
        return False

    def _update_drop_reasons(self, df):
        vol_1m_min = df['Volume'].rolling(window=20).min().iloc[-1]
        vol_1m_avg = df['Volume'].rolling(window=20).mean().iloc[-1]
        latest = df.iloc[-1]
        
        sma200_20d_ago = df['SMA200'].shift(20).iloc[-1]

        if vol_1m_min < 100000:
            self.drop_reasons["02_基本流動性不足(1ヶ月最低10万株)"] += 1
            return
            
        if latest['Close'] < latest['SMA200'] or latest['SMA200'] < sma200_20d_ago:
            self.drop_reasons["03_事前の長期上昇トレンドがない(200日線未満または下向き)"] += 1
            return

        # ★ 修正: 昨日までのデータで形を評価する
        cup_data = df.iloc[-250:-1] 

        high_val = cup_data['High'].max()
        high_idx = cup_data['High'].values.argmax()
        days_since_high = len(cup_data) - high_idx

        if days_since_high < 30:
            self.drop_reasons["04_最高値が近すぎる(カップの期間不足)"] += 1
            return

        past_after_high = cup_data.iloc[high_idx:]
        cup_low = past_after_high['Low'].min()
        cup_low_idx = past_after_high['Low'].values.argmin() + high_idx

        depth = (high_val - cup_low) / high_val
        if not (0.10 <= depth <= 0.45):
            self.drop_reasons["05_カップの深さが規格外(調整が10%〜45%ではない)"] += 1
            return

        right_side_data = cup_data.iloc[cup_low_idx:]
        right_peak_val = right_side_data['High'].max()

        if right_peak_val < high_val * 0.90:
            self.drop_reasons["06_右側の回復が不十分(元の高値付近まで戻っていない)"] += 1
            return

        right_peak_idx = right_side_data['High'].values.argmax()
        handle_data = right_side_data.iloc[right_peak_idx:]

        if len(handle_data) < 3:
            self.drop_reasons["07_取っ手(ハンドル)を形成する期間がない"] += 1
            return

        handle_low = handle_data['Low'].min()
        if handle_low < (high_val - (high_val - cup_low) / 2):
            self.drop_reasons["08_ハンドルの位置が低い(カップの下半分まで落ちている)"] += 1
            return

        if handle_data['Volume'].mean() >= vol_1m_avg:
            self.drop_reasons["09_取っ手での売り枯れがない(出来高が平均以上)"] += 1
            return

        if (right_peak_val / handle_low - 1) > 0.10:
            self.drop_reasons["10_取っ手のボラティリティが高すぎる(振れ幅10%超)"] += 1
            return

        # ★ 修正: ブレイクアウトの判定
        if latest['Close'] <= right_peak_val:
            self.drop_reasons["11_ハンドルの高値をブレイクアウトしていない(初動ではない)"] += 1
            return
