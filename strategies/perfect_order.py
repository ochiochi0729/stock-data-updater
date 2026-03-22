import pandas as pd
import numpy as np

class PerfectOrderScreener:
    def __init__(self):
        # インスタンス変数として初期化するよう変更 (@classmethod を廃止)
        self.drop_reasons = {
            "1_データ不足(100日)": 0, 
            "2_基本流動性不足(1ヶ月最低10万株)": 0,
            "3_パーフェクトオーダーの並びではない": 0, 
            "4_移動平均線が上向きではない(トレンド弱)": 0,
            "5a_数日前に十分な上昇(SMA25の+3%以上)がない": 0,  # MA25 -> SMA25に変更
            "5b_直近高値からの下落が大きすぎる(10%以上の暴落)": 0,
            "6_現在価格が25日線の押し目範囲外(0%〜2%)": 0, 
            "7_直近の出来高が細っていない(売り枯れ未完了)": 0,
            "8_反発の事実がない(前日比プラスの陽線等がない)": 0, 
            "9_異常なボラティリティ(決算等のノイズ回避)": 0
        }

    def reset_reasons(self):
        for key in self.drop_reasons:
            self.drop_reasons[key] = 0

    def check_conditions(self, df):
        # 1. データ不足
        if len(df) < 100:
            self.drop_reasons["1_データ不足(100日)"] += 1
            return False
            
        # ---------------------------------------------------------
        # DBに無い指標をスクリーニング時に動的に計算 (window=20 で約1ヶ月分)
        # ---------------------------------------------------------
        vol_1m_min = df['Volume'].rolling(window=20).min().iloc[-1]
        vol_1m_avg = df['Volume'].rolling(window=20).mean().iloc[-1]
        
        # カラム名を SMA25 / SMA75 に変更
        sma25_5d_ago = df['SMA25'].shift(5).iloc[-1]
        sma75_5d_ago = df['SMA75'].shift(5).iloc[-1]
        
        # 前日時点の平均レンジ (1ヶ月間の 高値 - 安値 の平均)
        avg_range_prev = (df['High'] - df['Low']).rolling(window=20).mean().shift(1).iloc[-1]
        
        latest = df.iloc[-1]
        prev = df.iloc[-2]

        # 2. 基本流動性不足
        if vol_1m_min < 100000:
            self.drop_reasons["2_基本流動性不足(1ヶ月最低10万株)"] += 1
            return False

        # 3. パーフェクトオーダーの並びではない (SMA25 > SMA75)
        if latest['SMA25'] <= latest['SMA75']:
            self.drop_reasons["3_パーフェクトオーダーの並びではない"] += 1
            return False

        # 4. 移動平均線が上向きではない
        if (latest['SMA25'] <= sma25_5d_ago) or (latest['SMA75'] <= sma75_5d_ago):
            self.drop_reasons["4_移動平均線が上向きではない(トレンド弱)"] += 1
            return False

        # 5a. 数日前の十分な上昇
        highs_recent = df['High'].iloc[-15:-2]
        sma25_recent = df['SMA25'].iloc[-15:-2]
        if not (highs_recent > sma25_recent * 1.03).any():
            self.drop_reasons["5a_数日前に十分な上昇(SMA25の+3%以上)がない"] += 1
            return False
            
        # 5b. 直近高値からの下落率
        recent_max = highs_recent.max()
        if pd.isna(recent_max) or recent_max == 0:
            self.drop_reasons["5b_直近高値からの下落が大きすぎる(10%以上の暴落)"] += 1
            return False

        drop_rate = (latest['Low'] / recent_max) - 1
        if drop_rate < -0.10:
            self.drop_reasons["5b_直近高値からの下落が大きすぎる(10%以上の暴落)"] += 1
            return False

        # 6. 現在価格が25日線の押し目範囲外
        current_vs_sma25 = (latest['Close'] / latest['SMA25']) - 1
        if not (0.0 <= current_vs_sma25 <= 0.02):
            self.drop_reasons["6_現在価格が25日線の押し目範囲外(0%〜2%)"] += 1
            return False

        # 7. 直近の出来高が細っていない
        recent_vol_mean = df['Volume'].iloc[-3:].mean()
        if recent_vol_mean >= vol_1m_avg:
            self.drop_reasons["7_直近の出来高が細っていない(売り枯れ未完了)"] += 1
            return False

        # 8. 反発の事実がない
        is_rebound = (latest['Close'] > prev['High']) or \
                     (latest['Close'] > latest['Open'] and latest['Close'] > prev['Close'])
        if not is_rebound:
            self.drop_reasons["8_反発の事実がない(前日比プラスの陽線等がない)"] += 1
            return False

        # 9. 異常なボラティリティ
        if prev['Close'] == 0:  # ゼロ除算回避
            self.drop_reasons["9_異常なボラティリティ(決算等のノイズ回避)"] += 1
            return False
            
        price_spread = latest['High'] - latest['Low']
        gap_ratio = abs((latest['Open'] / prev['Close']) - 1)
        
        if (price_spread > avg_range_prev * 2.5) or (gap_ratio > 0.04):
            self.drop_reasons["9_異常なボラティリティ(決算等のノイズ回避)"] += 1
            return False

        return True
