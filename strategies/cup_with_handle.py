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
        """【バックテスト用】全日程のCwHシグナルを一括計算"""
        if len(df) < LOOKBACK_DAYS:
            return pd.Series(False, index=df.index)

        close, vol = df['Close'], df['Volume']
        sma200 = close.rolling(200).mean()
        vol_1m_avg = vol.rolling(20).mean()
        vol_1m_min = vol.rolling(20).min()

        # 条件2, 3 (ベクトル演算)
        c2 = vol_1m_min >= 100000
        c3 = (close > sma200) & (sma200 > sma200.shift(20))

        signals = pd.Series(False, index=df.index)
        
        # カップ・ハンドル判定 (時系列ループ)
        for i in range(LOOKBACK_DAYS, len(df)):
            window = df.iloc[i-249 : i+1] # 直近250日
            if self._check_logic_pure(window, vol_1m_avg.iloc[i]):
                signals.iloc[i] = True
        
        return signals & c2 & c3

    def _check_logic_pure(self, cup_data, v_avg):
        """ロジックの本体（数値判定のみ）"""
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
        """【日次本番用】 run_screener.py はここを呼び出します"""
        if len(df) < LOOKBACK_DAYS:
            self.drop_reasons[f"01_データ不足({LOOKBACK_DAYS}日)"] += 1
            return False
            
        # 1. まず一括計算関数で最新日の結果(True/False)だけを取得
        # ※ 最新日の1点だけを計算するより、内部のベクトル演算を利用した方が安全かつ高速です
        sig = self.get_all_signals(df)
        if not sig.empty and sig.iloc[-1]: 
            return True
        
        # 2. 抽出されなかった場合のみ、脱落理由をカウントするために詳細ロジックを回す
        self._update_drop_reasons(df)
        return False

    def _update_drop_reasons(self, df):
        """日次レポートの「脱落理由」をカウントするためだけの処理（ロジックは完全維持）"""
        vol_1m_min = df['Volume'].rolling(window=20).min().iloc[-1]
        vol_1m_avg = df['Volume'].rolling(window=20).mean().iloc[-1]
        latest = df.iloc[-1]
        
        # =========================================================
        # ★ 修正箇所: SMA200をデータベースから呼ぶのではなく、その場で計算する
        # =========================================================
        sma200_series = df['Close'].rolling(window=200).mean()
        latest_sma200 = sma200_series.iloc[-1]
        sma200_20d_ago = sma200_series.shift(20).iloc[-1]

        if vol_1m_min < 100000:
            self.drop_reasons["02_基本流動性不足(1ヶ月最低10万株)"] += 1
            return
            
        # ★ 修正箇所: 計算した latest_sma200 を使って判定（ロジック自体は不変）
        if latest['Close'] < latest_sma200 or latest_sma200 < sma200_20d_ago:
            self.drop_reasons["03_事前の長期上昇トレンドがない(200日線未満または下向き)"] += 1
            return

        cup_data = df.iloc[-250:]
        high_val = cup_data['High'].max()
        high_idx = cup_data['High'].values.argmax()
        days_since_high = len(cup_data) - 1 - high_idx

        if days_since_high < 30:
            self.drop_reasons["04_最高値が近すぎる(カップの期間不足)"] += 1
            return

        cup_low = cup_data['Low'].min()
        depth = (high_val - cup_low) / high_val
        if not (0.10 <= depth <= 0.45):
            self.drop_reasons["05_カップの深さが規格外(調整が10%〜45%ではない)"] += 1
            return

        cup_low_idx = cup_data['Low'].values.argmin()
        right_side_data = cup_data.iloc[cup_low_idx:]
        right_edge_high = right_side_data['High'].max()

        if right_edge_high < high_val * 0.90:
            self.drop_reasons["06_右側の回復が不十分(元の高値付近まで戻っていない)"] += 1
            return

        handle_data = right_side_data.iloc[right_side_data['High'].values.argmax():]
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

        if (handle_data['High'].max() / handle_low - 1) > 0.10:
            self.drop_reasons["10_取っ手のボラティリティが高すぎる(振れ幅10%超)"] += 1
            return

        current_vs_high = (latest['Close'] / high_val) - 1
        if not (-0.05 <= current_vs_high <= 0.01):
            self.drop_reasons["11_現在価格が最高値から離れている(-5%〜0%の範囲外)"] += 1
            return
