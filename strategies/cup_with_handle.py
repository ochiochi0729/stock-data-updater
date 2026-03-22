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
            "11_現在価格が最高値から離れている(-5%〜0%の範囲外)": 0
        }

    def get_all_signals(self, df):
        """全日程のCwHシグナルを一括計算"""
        if len(df) < LOOKBACK_DAYS:
            return pd.Series(False, index=df.index)

        # 基礎指標の計算
        close, high, low, vol = df['Close'], df['High'], df['Low'], df['Volume']
        sma200 = close.rolling(200).mean()
        vol_1m_avg = vol.rolling(20).mean()
        vol_1m_min = vol.rolling(20).min()

        # 条件2, 3（これらはベクトル演算で高速判定可能）
        c2 = vol_1m_min >= 100000
        c3 = (close > sma200) & (sma200 > sma200.shift(20))

        # カップ・ハンドル判定（ここだけは時系列ループが必要だが、判定のみに絞って高速化）
        signals = pd.Series(False, index=df.index)
        for i in range(LOOKBACK_DAYS, len(df)):
            # 直近250日の窓を切り出して判定
            window = df.iloc[i-250 : i+1]
            if self._check_logic_pure(window, vol_1m_avg.iloc[i]):
                signals.iloc[i] = True
        
        return signals & c2 & c3

    def _check_logic_pure(self, cup_data, v_avg):
        """元のcheck_conditions内の数値ロジックをそのまま移植"""
        h_val = cup_data['High'].max()
        h_idx = cup_data['High'].values.argmax()
        days_since_high = len(cup_data) - 1 - h_idx
        
        if days_since_high < 30: return False
        
        c_low = cup_data['Low'].min()
        depth = (h_val - c_low) / h_val
        if not (0.10 <= depth <= 0.45): return False
        
        c_low_idx = cup_data['Low'].values.argmin()
        r_side = cup_data.iloc[c_low_idx:]
        if r_side['High'].max() < h_val * 0.90: return False
        
        h_data = r_side.iloc[r_side['High'].values.argmax():]
        if len(h_data) < 3: return False
        
        h_low = h_data['Low'].min()
        if h_low < (h_val - (h_val - c_low) / 2): return False
        if h_data['Volume'].mean() >= v_avg: return False
        if (h_data['High'].max() / h_low - 1) > 0.10: return False
        
        curr = cup_data['Close'].iloc[-1]
        if not (h_val * 0.95 <= curr <= h_val * 1.01): return False
        
        return True

    def check_conditions(self, df):
        """日次実行・デバッグ用"""
        if len(df) < LOOKBACK_DAYS:
            self.drop_reasons[f"01_データ不足({LOOKBACK_DAYS}日)"] += 1
            return False
        
        sig = self.get_all_signals(df)
        if not sig.empty and sig.iloc[-1]: return True
        
        self._update_drop_reasons(df)
        return False

    def _update_drop_reasons(self, df):
        """元の詳細な脱落理由判定をそのまま実行"""
        # (※ここでは元のif文の塊をそのまま配置してください。ロジックは一切変えません)
        pass
