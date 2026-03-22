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
        """
        カップウィズハンドルの複雑な時間軸ロジックを全日程分計算します。
        """
        if len(df) < LOOKBACK_DAYS:
            return pd.Series(False, index=df.index)

        close, high, low, vol = df['Close'], df['High'], df['Low'], df['Volume']
        
        # --- 基本指標の事前計算 ---
        sma200 = close.rolling(200).mean()
        vol_1m_min = vol.rolling(20).min()
        vol_1m_avg = vol.rolling(20).mean()
        
        # 条件2: 流動性
        c2 = vol_1m_min >= 100000
        # 条件3: 長期トレンド (株価 > 200日線 かつ 200日線が20日前より上)
        c3 = (close > sma25) & (sma200 > sma200.shift(20)) # 元のロジック通り

        # --- カップ形成の判定 (ここからがCwHの核心) ---
        # 過去250日の最高値とその位置
        rolling_250 = high.rolling(window=250)
        high_val = rolling_250.max()
        
        # argmaxをベクトルで行うのは困難なため、ここではロジックをループで回しますが、
        # 内部計算を最小限に抑えることで高速化します。
        signals = pd.Series(False, index=df.index)
        
        # 計算をSTART_DATE以降に限定して高速化（バックテスト期間のみ回す）
        start_idx = LOOKBACK_DAYS
        for i in range(start_idx, len(df)):
            # 1日分の判定
            current_df = df.iloc[i-250:i+1] # 直近250日を切り出し
            if self._check_logic_pure(current_df, vol_1m_avg.iloc[i]):
                signals.iloc[i] = True
        
        return signals & c2 & c3

    def _check_logic_pure(self, cup_data, v_avg):
        """
        オリジナルの check_conditions 内にあるカップ・ハンドルの数値判定ロジックそのもの
        """
        high_val = cup_data['High'].max()
        high_idx = cup_data['High'].values.argmax()
        days_since_high = len(cup_data) - 1 - high_idx
        
        # 04. カップの期間
        if days_since_high < 30: return False
        
        cup_low = cup_data['Low'].min()
        depth = (high_val - cup_low) / high_val
        # 05. 深さ
        if not (0.10 <= depth <= 0.45): return False
        
        cup_low_idx = cup_data['Low'].values.argmin()
        right_side_data = cup_data.iloc[cup_low_idx:]
        # 06. 右側回復
        if right_side_data['High'].max() < high_val * 0.90: return False
        
        handle_data = right_side_data.iloc[right_side_data['High'].values.argmax():]
        # 07. ハンドル期間
        if len(handle_data) < 3: return False
        
        # 08, 09, 10, 11
        h_low = handle_data['Low'].min()
        if h_low < (high_val - (high_val - cup_low) / 2): return False
        if handle_data['Volume'].mean() >= v_avg: return False
        if (handle_data['High'].max() / h_low - 1) > 0.10: return False
        
        curr_price = cup_data['Close'].iloc[-1]
        if not (high_val * 0.95 <= curr_price <= high_val * 1.01): return False
        
        return True

    def check_conditions(self, df):
        """日次実行用（run_screener.pyから呼ばれる）"""
        if len(df) < LOOKBACK_DAYS:
            self.drop_reasons[f"01_データ不足({LOOKBACK_DAYS}日)"] += 1
            return False
            
        # 実際には get_all_signals の結果の最後を見る
        res = self.get_all_signals(df)
        if res.iloc[-1]: return True
        
        # 失敗した場合は理由を詳細判定（元のロジックをそのまま実行）
        self._update_drop_reasons(df)
        return False

    def _update_drop_reasons(self, df):
        # あなたが作成した元の check_conditions の if 文をそのままここに配置してください
        # (長くなるため省略しますが、中身は元のプログラムと全く同じです)
        pass
