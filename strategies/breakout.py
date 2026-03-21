class BreakoutScreener:
    drop_reasons = {
        "1_データ不足": 0, "2_基本流動性不足": 0,
        "3a_75日線が過去に下降トレンドではない(上昇中など)": 0, "3b_75日線の直近の下降が落ち着いていない(±1.5%の範囲外)": 0, 
        "3c_25日線と75日線が離れすぎている(底練り未完了)": 0, "4_25日線が下向き": 0,
        "5_直近で下から上抜けしていない": 0, "6_価格が25日線の範囲外(-1%〜+5%)": 0, 
        "7_直近5日間の上昇＆出来高急増なし": 0, "8_価格が75日線の下にある(上値抵抗線のダマシ回避)": 0
    }
    @classmethod
    def reset_reasons(cls):
        for key in cls.drop_reasons: cls.drop_reasons[key] = 0
    @classmethod
    def check_conditions(cls, df):
        if len(df) < 80:
            cls.drop_reasons["1_データ不足"] += 1; return False
        latest = df.iloc[-1]
        if latest['Vol_1M_min'] < 100000:
            cls.drop_reasons["2_基本流動性不足"] += 1; return False
        ma75_40d_ago, ma75_10d_ago = df['MA75'].iloc[-40], latest['MA75_10d_ago']
        if ma75_40d_ago <= ma75_10d_ago:
            cls.drop_reasons["3a_75日線が過去に下降トレンドではない(上昇中など)"] += 1; return False
        if not (-0.015 <= ((latest['MA75'] / ma75_10d_ago) - 1) <= 0.015):
            cls.drop_reasons["3b_75日線の直近の下降が落ち着いていない(±1.5%の範囲外)"] += 1; return False
        if abs(latest['MA25'] - latest['MA75']) / latest['MA75'] > 0.05:
            cls.drop_reasons["3c_25日線と75日線が離れすぎている(底練り未完了)"] += 1; return False
        if ((latest['MA25'] / latest['MA25_3d_ago']) - 1) < -0.001:
            cls.drop_reasons["4_25日線が下向き"] += 1; return False
        if not (df['Close'].iloc[-6:-1] < df['MA25'].iloc[-6:-1]).any():
            cls.drop_reasons["5_直近で下から上抜けしていない"] += 1; return False
        if not (0.99 <= (latest['Close'] / latest['MA25']) <= 1.05):
            cls.drop_reasons["6_価格が25日線の範囲外(-1%〜+5%)"] += 1; return False
        has_vol = False
        for i in range(1, 6):
            if (df['Volume'].iloc[-i] >= df['Vol_1M'].iloc[-i] * 1.5) and ((df['Close'].iloc[-i] > df['Open'].iloc[-i]) or (df['Close'].iloc[-i] > df['Close_1d_ago'].iloc[-i])):
                has_vol = True; break
        if not has_vol:
            cls.drop_reasons["7_直近5日間の上昇＆出来高急増なし"] += 1; return False
        if latest['Close'] <= latest['MA75']:
            cls.drop_reasons["8_価格が75日線の下にある(上値抵抗線のダマシ回避)"] += 1; return False
        return True
