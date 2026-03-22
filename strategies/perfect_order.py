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

    def get_all_signals(self, df):
        """
        元の check_conditions と1円単位まで計算結果を一致させるため、
        スライス範囲や条件式を完全に同期させたベクトル演算です。
        """
        if len(df) < 100:
            return pd.Series(False, index=df.index)

        c, o, h, l, v = df['Close'], df['Open'], df['High'], df['Low'], df['Volume']
        s25, s75 = df['SMA25'], df['SMA75']

        # 2. 流動性
        c2 = v.rolling(20).min() >= 100000
        # 3. PO
        c3 = s25 > s75
        # 4. MA向き
        c4 = (s25 > s25.shift(5)) & (s75 > s75.shift(5))
        
        # 5a & 5b. 直近高値 (15日前〜2日前 = 13日間)
        # shift(2).rolling(13) で、当日を含まない「2日前から15日前まで」をカバー
        is_surge = (h > s25 * 1.03)
        c5a = is_surge.shift(2).rolling(13).max().fillna(0).astype(bool)
        
        recent_max = h.shift(2).rolling(13).max()
        c5b = (l / recent_max) >= 0.90
        
        # 6. 押し目
        diff = (c / s25) - 1
        c6 = (diff >= 0.0) & (diff <= 0.02)
        # 7. 売り枯れ (3日平均 < 20日平均)
        c7 = v.rolling(3).mean() < v.rolling(20).mean()
        # 8. 反発
        c8 = (c > h.shift(1)) | ((c > o) & (c > c.shift(1)))
        
        # 9. ボラティリティ
        avg_range_prev = (h - l).rolling(20).mean().shift(1)
        gap = (o / c.shift(1) - 1).abs()
        c9 = ~(((h - l) > avg_range_prev * 2.5) | (gap > 0.04))

        return c2 & c3 & c4 & c5a & c5b & c6 & c7 & c8 & c9

    def check_conditions(self, df):
        """本番スクリーニング用（脱落理由のカウントを維持）"""
        if len(df) < 100:
            self.drop_reasons["1_データ不足(100日)"] += 1
            return False
            
        # get_all_signals の最新結果を取得
        all_res = self.get_all_signals(df)
        if all_res.iloc[-1]:
            return True
        
        # 理由をカウントするために元の if 文ロジックを実行
        self._update_drop_reasons(df)
        return False

    def _update_drop_reasons(self, df):
        # ここに、あなたが作成した元の check_conditions 内の if 文をそのまま配置します
        # (run_screener.py でのレポート表示を維持するため)
        vol_1m_min = df['Volume'].rolling(20).min().iloc[-1]
        vol_1m_avg = df['Volume'].rolling(20).mean().iloc[-1]
        sma25_5d_ago = df['SMA25'].shift(5).iloc[-1]
        sma75_5d_ago = df['SMA75'].shift(5).iloc[-1]
        avg_range_prev = (df['High'] - df['Low']).rolling(20).mean().shift(1).iloc[-1]
        latest, prev = df.iloc[-1], df.iloc[-2]

        if vol_1m_min < 100000: self.drop_reasons["2_基本流動性不足(1ヶ月最低10万株)"] += 1
        elif latest['SMA25'] <= latest['SMA75']: self.drop_reasons["3_パーフェクトオーダーの並びではない"] += 1
        elif (latest['SMA25'] <= sma25_5d_ago) or (latest['SMA75'] <= sma75_5d_ago): self.drop_reasons["4_移動平均線が上向きではない(トレンド弱)"] += 1
        elif not (df['High'].iloc[-15:-2] > df['SMA25'].iloc[-15:-2] * 1.03).any(): self.drop_reasons["5a_数日前に十分な上昇(SMA25の+3%以上)がない"] += 1
        elif (latest['Low'] / df['High'].iloc[-15:-2].max()) < 0.90: self.drop_reasons["5b_直近高値からの下落が大きすぎる(10%以上の暴落)"] += 1
        elif not (0.0 <= (latest['Close'] / latest['SMA25']) - 1 <= 0.02): self.drop_reasons["6_現在価格が25日線の押し目範囲外(0%〜2%)"] += 1
        elif df['Volume'].iloc[-3:].mean() >= vol_1m_avg: self.drop_reasons["7_直近の出来高が細っていない(売り枯れ未完了)"] += 1
        elif not ((latest['Close'] > prev['High']) or (latest['Close'] > latest['Open'] and latest['Close'] > prev['Close'])): self.drop_reasons["8_反発の事実がない(前日比プラスの陽線等がない)"] += 1
        elif (latest['High'] - latest['Low'] > avg_range_prev * 2.5) or (abs(latest['Open']/prev['Close'] - 1) > 0.04): self.drop_reasons["9_異常なボラティリティ(決算等のノイズ回避)"] += 1
