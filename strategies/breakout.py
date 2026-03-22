import pandas as pd
import numpy as np

class BreakoutScreener:
    def __init__(self):
        self.drop_reasons = {}
        self.reset_reasons()

    def reset_reasons(self):
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

    def _calculate_all_conditions(self, df):
        """元の if 文のロジックを、完全に同じ意味のベクトル演算に変換"""
        if len(df) < 80: return None

        c, v = df['Close'], df['Volume']
        s25, s75 = df['SMA25'], df['SMA75']

        # 2. 基本流動性不足
        c2 = v.rolling(20).min() >= 100000

        # 3. 75日線と25日線の関係
        c3a = s75 >= s75.shift(30)
        c3b = (s75 / s75.shift(10) - 1).abs() <= 0.015
        c3c = (s25 / s75 - 1).abs() <= 0.05

        # 4. 25日線が下向き (ゼロ除算回避を含む)
        c4 = (s25.shift(3) != 0) & (((s25 / s25.shift(3)) - 1) >= -0.001)

        # 5. 直近で下から上抜けしていない (1日前〜5日前の間に Close < SMA25 となる日が1日以上あるか)
        is_under = c < s25
        c5 = is_under.shift(1).rolling(5).max().fillna(0).astype(bool)

        # 6. 価格が25日線の範囲外
        c6 = (s25 != 0) & ((c / s25) >= 0.99) & ((c / s25) <= 1.05)

        # 7. 直近5日間の上昇＆出来高急増なし (当日〜4日前までの間に1日以上あるか)
        vol_1m_avg = v.rolling(20).mean()
        vol_surge = v >= vol_1m_avg * 1.5
        price_surge = c > c.shift(1) * 1.02
        is_surge = vol_surge & price_surge
        c7 = is_surge.rolling(5).max().fillna(0).astype(bool)

        # 8. 価格が75日線の下にある
        c8 = c >= s75

        return {"c2":c2, "c3a":c3a, "c3b":c3b, "c3c":c3c, "c4":c4, "c5":c5, "c6":c6, "c7":c7, "c8":c8}

    def get_all_signals(self, df):
        """【バックテスト用】全日程のシグナルを一括計算"""
        conds = self._calculate_all_conditions(df)
        if conds is None: return pd.Series(False, index=df.index)
        return conds["c2"] & conds["c3a"] & conds["c3b"] & conds["c3c"] & conds["c4"] & conds["c5"] & conds["c6"] & conds["c7"] & conds["c8"]

    def check_conditions(self, df):
        """【日次本番用】 run_screener.py はここを呼び出します"""
        conds = self._calculate_all_conditions(df)
        if conds is None:
            self.drop_reasons["1_データ不足"] += 1
            return False
            
        res = {k: v.iloc[-1] for k, v in conds.items()}
        
        # 抽出された場合はTrueを返す
        if all(res.values()):
            return True
            
        # 抽出されなかった場合、元のロジックを回して理由をカウントする
        self._update_drop_reasons(df)
        return False

    def _update_drop_reasons(self, df):
        """あなたが作成した元の判定ロジックをそのまま実行し、理由をカウントします"""
        vol_1m_min = df['Volume'].rolling(window=20).min().iloc[-1]
        vol_1m_avg = df['Volume'].rolling(window=20).mean()
        
        sma75_30d_ago = df['SMA75'].shift(30).iloc[-1]
        sma75_10d_ago = df['SMA75'].shift(10).iloc[-1]
        sma25_3d_ago = df['SMA25'].shift(3).iloc[-1]
        
        latest = df.iloc[-1]

        if vol_1m_min < 100000:
            self.drop_reasons["2_基本流動性不足"] += 1
            return
            
        if latest['SMA75'] < sma75_30d_ago:
            self.drop_reasons["3a_75日線が過去に下降トレンドではない(上昇中など)"] += 1
            return
            
        if sma75_10d_ago == 0 or abs((latest['SMA75'] / sma75_10d_ago) - 1) > 0.015:
            self.drop_reasons["3b_75日線の直近の下降が落ち着いていない(±1.5%の範囲外)"] += 1
            return
            
        if latest['SMA75'] == 0 or abs((latest['SMA25'] / latest['SMA75']) - 1) > 0.05:
            self.drop_reasons["3c_25日線と75日線が離れすぎている(底練り未完了)"] += 1
            return
            
        if sma25_3d_ago == 0 or ((latest['SMA25'] / sma25_3d_ago) - 1) < -0.001:
            self.drop_reasons["4_25日線が下向き"] += 1
            return
            
        if not (df['Close'].iloc[-6:-1] < df['SMA25'].iloc[-6:-1]).any():
            self.drop_reasons["5_直近で下から上抜けしていない"] += 1
            return
            
        if latest['SMA25'] == 0 or not (0.99 <= (latest['Close'] / latest['SMA25']) <= 1.05):
            self.drop_reasons["6_価格が25日線の範囲外(-1%〜+5%)"] += 1
            return
            
        has_vol = False
        for i in range(1, 6):
            vol_surge = df['Volume'].iloc[-i] >= vol_1m_avg.iloc[-i] * 1.5
            price_surge = df['Close'].iloc[-i] > df['Close'].iloc[-i-1] * 1.02
            if vol_surge and price_surge:
                has_vol = True
                break
        
        if not has_vol:
            self.drop_reasons["7_直近5日間の上昇＆出来高急増なし"] += 1
            return
            
        if latest['Close'] < latest['SMA75']:
            self.drop_reasons["8_価格が75日線の下にある(上値抵抗線のダマシ回避)"] += 1
            return
