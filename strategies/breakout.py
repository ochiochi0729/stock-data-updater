import pandas as pd
import numpy as np

class BreakoutScreener:
    def __init__(self):
        self.drop_reasons = {
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

    def reset_reasons(self):
        for key in self.drop_reasons:
            self.drop_reasons[key] = 0

    def check_conditions(self, df):
        # 1. データ不足 (40日前のデータを参照するため、余裕を持って80日以上を要求)
        if len(df) < 80:
            self.drop_reasons["1_データ不足"] += 1
            return False

        # ---------------------------------------------------------
        # スクリーニング時に動的に計算する指標群
        # ---------------------------------------------------------
        # 1ヶ月(20日)の出来高関連
        vol_1m_min = df['Volume'].rolling(window=20).min().iloc[-1]
        vol_1m_avg = df['Volume'].rolling(window=20).mean() # 過去に遡って比較するためシリーズで保持

        # 過去の移動平均線
        sma75_40d_ago = df['SMA75'].shift(40).iloc[-1]
        sma75_10d_ago = df['SMA75'].shift(10).iloc[-1]
        sma25_3d_ago = df['SMA25'].shift(3).iloc[-1]

        latest = df.iloc[-1]

        # 2. 基本流動性不足
        if vol_1m_min < 100000:
            self.drop_reasons["2_基本流動性不足"] += 1
            return False

        # 3a. 75日線が過去に下降トレンドではない
        if sma75_40d_ago <= sma75_10d_ago:
            self.drop_reasons["3a_75日線が過去に下降トレンドではない(上昇中など)"] += 1
            return False

        # 3b. 75日線の直近の下降が落ち着いていない（ゼロ除算回避含む）
        if sma75_10d_ago == 0 or not (-0.015 <= ((latest['SMA75'] / sma75_10d_ago) - 1) <= 0.015):
            self.drop_reasons["3b_75日線の直近の下降が落ち着いていない(±1.5%の範囲外)"] += 1
            return False

        # 3c. 25日線と75日線が離れすぎている (底練り未完了)
        if latest['SMA75'] == 0 or abs(latest['SMA25'] - latest['SMA75']) / latest['SMA75'] > 0.05:
            self.drop_reasons["3c_25日線と75日線が離れすぎている(底練り未完了)"] += 1
            return False

        # 4. 25日線が下向き
        if sma25_3d_ago == 0 or ((latest['SMA25'] / sma25_3d_ago) - 1) < -0.001:
            self.drop_reasons["4_25日線が下向き"] += 1
            return False

        # 5. 直近で下から上抜けしていない
        # iloc[-6:-1] は直近5日間のデータ（最新日を含まない）
        if not (df['Close'].iloc[-6:-1] < df['SMA25'].iloc[-6:-1]).any():
            self.drop_reasons["5_直近で下から上抜けしていない"] += 1
            return False

        # 6. 価格が25日線の範囲外
        if latest['SMA25'] == 0 or not (0.99 <= (latest['Close'] / latest['SMA25']) <= 1.05):
            self.drop_reasons["6_価格が25日線の範囲外(-1%〜+5%)"] += 1
            return False

        # 7. 直近5日間の上昇＆出来高急増なし
        has_vol = False
        for i in range(1, 6):
            # 出来高急増条件: 過去の該当日の出来高が、その日時点の1ヶ月平均出来高の1.5倍以上か
            vol_surge = df['Volume'].iloc[-i] >= vol_1m_avg.iloc[-i] * 1.5
            
            # 陽線または前日比プラス条件: その日の終値 > 始値 OR その日の終値 > 前日の終値
            # iloc[-i-1] で前日の終値を取得
            price_up = (df['Close'].iloc[-i] > df['Open'].iloc[-i]) or (df['Close'].iloc[-i] > df['Close'].iloc[-i-1])
            
            if vol_surge and price_up:
                has_vol = True
                break

        if not has_vol:
            self.drop_reasons["7_直近5日間の上昇＆出来高急増なし"] += 1
            return False

        # 8. 価格が75日線の下にある
        if latest['Close'] <= latest['SMA75']:
            self.drop_reasons["8_価格が75日線の下にある(上値抵抗線のダマシ回避)"] += 1
            return False

        return True
