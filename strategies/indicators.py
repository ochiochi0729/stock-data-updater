class IndicatorCalculator:
    @staticmethod
    def add_indicators(df):
        # PO用
        df['MA25'] = df['Close'].rolling(window=25).mean()
        df['MA75'] = df['Close'].rolling(window=75).mean()
        df['MA25_5d_ago'] = df['MA25'].shift(5)
        df['MA75_5d_ago'] = df['MA75'].shift(5)
        df['Avg_Range'] = (df['High'] - df['Low']).rolling(window=20).mean()
        # CwH用
        df['MA200'] = df['Close'].rolling(window=200).mean()
        df['MA200_20d_ago'] = df['MA200'].shift(20)
        # Breakout用
        df['MA25_3d_ago'] = df['MA25'].shift(3)
        df['MA75_10d_ago'] = df['MA75'].shift(10)
        df['Close_1d_ago'] = df['Close'].shift(1)
        # 共通
        df['Vol_1M'] = df['Volume'].rolling(window=20).mean()
        df['Vol_1M_min'] = df['Volume'].rolling(window=20).min()
        return df
