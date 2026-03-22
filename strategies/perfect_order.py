import pandas as pd
import numpy as np

class PerfectOrderScreener:
    def __init__(self):
        self.drop_reasons = {}
        self.reset_reasons()

    def reset_reasons(self):
        self.drop_reasons = {
            "1_データ不足(100日)": 0, 
            "2_基本流動性不足(1ヶ月最低10万株)": 0,
            "3_パーフェクトオーダーの並びではない": 0, 
            "4_移動平均線が上向きではない(トレンド弱)": 0,
            "5a_数日前に十分な上昇(SMA25の+3%以上)がない": 0, 
            "5b_直近高値からの下落が大きすぎる(10%以上の暴落)": 0,
            "6_現在価格が25日線の押し目範囲外(0%〜2%)": 0, 
            "7_直近の出来高が細っていない(売り枯れ未完了)": 0,
            "8_反発の事実がない(前日比プラスの陽線等がない)": 0, 
            "9_異常なボラティリティ(決算等のノイズ回避)": 0
        }

    def _calculate_all_conditions(self, df):
        """【唯一のロジック定義】全ての判定条件をベクトル演算で計算し、各条件のSeriesを返す"""
        if len(df) < 100:
            return None

        close, open_, high, low, vol = df['Close'], df['Open'], df['High'], df['Low'], df['Volume']
        sma25, sma75 = df['SMA25'], df['SMA75']

        # 2. 流動性
        c2 = vol.rolling(20).min() >= 100000
        # 3. PO
        c3 = sma25 > sma75
        # 4. MA向き
        c4 = (sma25 > sma25.shift(5)) & (sma75 > sma75.shift(5))
        # 5a. 15日前〜2日前の高値 vs SMA25*1.03
        h_recent_max = high.shift(2).rolling(13).max()
        s_recent_max = sma25.shift(2).rolling(13).max()
        c5a = h_recent_max > (s_recent_max * 1.03)
        # 5b. 下落率
        c5b = (low / h_recent_max) >= 0.90
        # 6. 押し目
        diff = (close / sma25) - 1
        c6 = (diff >= 0.0) & (diff <= 0.02)
        # 7. 売り枯れ
        c7 = vol.rolling(3).mean() < vol.rolling(20).mean()
        # 8. 反発
        c8 = (close > high.shift(1)) | ((close > open_) & (close > close.shift(1)))
        # 9. ボラティリティ
        avg_range_prev = (high - low).rolling(20).mean().shift(1)
        c9 = ~(((high - low) > avg_range_prev * 2.5) | ((open_ / close.shift(1) - 1).abs() > 0.04))

        return {"c2":c2, "c3":c3, "c4":c4, "c5a":c5a, "c5b":c5b, "c6":c6, "c7":c7, "c8":c8, "c9":c9}

    def get_all_signals(self, df):
        """バックテスト用：全条件を満たすフラグ列を返す"""
        conds = self._calculate_all_conditions(df)
        if conds is None: return pd.Series(False, index=df.index)
        return conds["c2"] & conds["c3"] & conds["c4"] & conds["c5a"] & conds["c5b"] & conds["c6"] & conds["c7"] & conds["c8"] & conds["c9"]

    def check_conditions(self, df):
        """日次用：最新日の判定と理由のカウント"""
        conds = self._calculate_all_conditions(df)
        if conds is None:
            self.drop_reasons["1_データ不足(100日)"] += 1
            return False

        # 最新日の各条件を確認してカウント（出力結果を以前と一致させる）
        results = {k: v.iloc[-1] for k, v in conds.items()}
        if not results["c2"]: self.drop_reasons["2_基本流動性不足(1ヶ月最低10万株)"] += 1
        elif not results["c3"]: self.drop_reasons["3_パーフェクトオーダーの並びではない"] += 1
        elif not results["c4"]: self.drop_reasons["4_移動平均線が上向きではない(トレンド弱)"] += 1
        elif not results["c5a"]: self.drop_reasons["5a_数日前に十分な上昇(SMA25の+3%以上)がない"] += 1
        elif not results["c5b"]: self.drop_reasons["5b_直近高値からの下落が大きすぎる(10%以上の暴落)"] += 1
        elif not results["c6"]: self.drop_reasons["6_現在価格が25日線の押し目範囲外(0%〜2%)"] += 1
        elif not results["c7"]: self.drop_reasons["7_直近の出来高が細っていない(売り枯れ未完了)"] += 1
        elif not results["c8"]: self.drop_reasons["8_反発の事実がない(前日比プラスの陽線等がない)"] += 1
        elif not results["c9"]: self.drop_reasons["9_異常なボラティリティ(決算等のノイズ回避)"] += 1
        else: return True
        return False
