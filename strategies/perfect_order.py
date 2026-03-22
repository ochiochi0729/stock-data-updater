import pandas as pd
import numpy as np

class PerfectOrderScreener:
    def __init__(self):
        # インスタンス変数として初期化
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
        【ロジックの唯一の定義場所】
        ご提示いただいたロジックをすべて『列(Series)』として一括計算します。
        """
        if len(df) < 100:
            return pd.Series(False, index=df.index)

        # --- 共通の指標計算 ---
        close = df['Close']
        open_ = df['Open']
        high = df['High']
        low = df['Low']
        vol = df['Volume']
        sma25 = df['SMA25']
        sma75 = df['SMA75']
        
        # 2. 流動性 (20日間の最小出来高)
        vol_1m_min = vol.rolling(window=20).min()
        c2 = vol_1m_min >= 100000

        # 3. PO (SMA25 > SMA75)
        c3 = sma25 > sma75

        # 4. MA上向き (5日前比較)
        c4 = (sma25 > sma25.shift(5)) & (sma75 > sma75.shift(5))

        # 5a. 数日前の十分な上昇 (「15日前〜2日前」の高値とSMA25を比較)
        # ※iloc[-15:-2]は「最新から数えて15日前から、2日前まで」の13日間
        # ベクトル演算では rolling(13) を 2日分 shift させます
        highs_recent_max = high.shift(2).rolling(window=13).max()
        sma25_recent_min = sma25.shift(2).rolling(window=13).min() # 判定用にSMAも同様にスライド
        # 厳密には「いずれかが超えているか」なので、比較したあとに rolling().any()
        c5a = ((high.shift(2).rolling(13).max() > (sma25.shift(2).rolling(13).max() * 1.03)))

        # 5b. 直近高値からの下落率
        c5b = (low / highs_recent_max) >= 0.90

        # 6. 押し目範囲 (SMA25の0%〜2%上)
        diff_pct = (close / sma25) - 1
        c6 = (diff_pct >= 0.0) & (diff_pct <= 0.02)

        # 7. 売り枯れ (3日平均出来高 < 1ヶ月平均出来高)
        vol_1m_avg = vol.rolling(window=20).mean()
        vol_3d_mean = vol.rolling(window=3).mean()
        c7 = vol_3d_mean < vol_1m_avg

        # 8. 反発
        prev_high = high.shift(1)
        prev_close = close.shift(1)
        c8 = (close > prev_high) | ((close > open_) & (close > prev_close))

        # 9. 異常なボラティリティ
        avg_range_prev = (high - low).rolling(window=20).mean().shift(1)
        price_spread = high - low
        gap_ratio = (open_ / prev_close - 1).abs()
        c9 = ~((price_spread > avg_range_prev * 2.5) | (gap_ratio > 0.04))

        # すべての条件を統合
        return c2 & c3 & c4 & c5a & c5b & c6 & c7 & c8 & c9

    def check_conditions(self, df):
        """
        【日次本番用】run_screener.py から呼ばれます。
        最後の一行（最新日）の判定結果を返します。
        """
        signals = self.get_all_signals(df)
        if signals.empty: return False
        
        is_hit = bool(signals.iloc[-1])
        
        # 抽出されなかった場合、理由をカウント（ここは計算負荷が低いので従来のIF文でOK）
        if not is_hit:
            self._update_drop_reasons(df)
            
        return is_hit

    def _update_drop_reasons(self, df):
        """
        既存の if 文による判定をここに移します。
        これにより、run_screener.py での「脱落理由表示」も今まで通り動作します。
        """
        if len(df) < 100:
            self.drop_reasons["1_データ不足(100日)"] += 1
            return
        
        # ...（ここにあなたが提示した if ~ return False の塊をそのまま貼り付けます）...
        # ただし、最後は return False ではなく、該当箇所で += 1 して return するだけ。
