class PerfectOrderScreener:
    drop_reasons = {
        "1_データ不足(100日)": 0, "2_基本流動性不足(1ヶ月最低10万株)": 0,
        "3_パーフェクトオーダーの並びではない": 0, "4_移動平均線が上向きではない(トレンド弱)": 0,
        "5a_数日前に十分な上昇(MA25の+3%以上)がない": 0, "5b_直近高値からの下落が大きすぎる(10%以上の暴落)": 0,
        "6_現在価格が25日線の押し目範囲外(0%〜2%)": 0, "7_直近の出来高が細っていない(売り枯れ未完了)": 0,
        "8_反発の事実がない(前日比プラスの陽線等がない)": 0, "9_異常なボラティリティ(決算等のノイズ回避)": 0
    }
    @classmethod
    def reset_reasons(cls):
        for key in cls.drop_reasons: cls.drop_reasons[key] = 0
    @classmethod
    def check_conditions(cls, df):
        if len(df) < 100:
            cls.drop_reasons["1_データ不足(100日)"] += 1
            return False
        latest, prev = df.iloc[-1], df.iloc[-2]
        if latest['Vol_1M_min'] < 100000:
            cls.drop_reasons["2_基本流動性不足(1ヶ月最低10万株)"] += 1; return False
        if latest['MA25'] <= latest['MA75']:
            cls.drop_reasons["3_パーフェクトオーダーの並びではない"] += 1; return False
        if (latest['MA25'] <= latest['MA25_5d_ago']) or (latest['MA75'] <= latest['MA75_5d_ago']):
            cls.drop_reasons["4_移動平均線が上向きではない(トレンド弱)"] += 1; return False
        highs_recent, ma25_recent = df['High'].iloc[-15:-2], df['MA25'].iloc[-15:-2]
        if not (highs_recent > ma25_recent * 1.03).any():
            cls.drop_reasons["5a_数日前に十分な上昇(MA25の+3%以上)がない"] += 1; return False
        drop_rate = (latest['Low'] / highs_recent.max()) - 1
        if drop_rate < -0.10:
            cls.drop_reasons["5b_直近高値からの下落が大きすぎる(10%以上の暴落)"] += 1; return False
        current_vs_ma25 = (latest['Close'] / latest['MA25']) - 1
        if not (-0.00 <= current_vs_ma25 <= 0.02):
            cls.drop_reasons["6_現在価格が25日線の押し目範囲外(0%〜2%)"] += 1; return False
        if df['Volume'].iloc[-3:].mean() >= latest['Vol_1M']:
            cls.drop_reasons["7_直近の出来高が細っていない(売り枯れ未完了)"] += 1; return False
        if not ((latest['Close'] > prev['High']) or (latest['Close'] > latest['Open'] and latest['Close'] > prev['Close'])):
            cls.drop_reasons["8_反発の事実がない(前日比プラスの陽線等がない)"] += 1; return False
        if (latest['High'] - latest['Low'] > prev['Avg_Range'] * 2.5) or (abs((latest['Open'] / prev['Close']) - 1) > 0.04):
            cls.drop_reasons["9_異常なボラティリティ(決算等のノイズ回避)"] += 1; return False
        return True
