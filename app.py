import streamlit as st
import pandas as pd
from google.cloud import bigquery
import plotly.graph_objects as go
import os

# --- 1. ページの設定 ---
st.set_page_config(page_title="株価ダッシュボード", layout="wide")

# --- 2. BigQueryの認証設定 ---
# お持ちのJSONキーのファイル名を指定してください
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "stock-data-updater-490714-2849f80d10f1.json"

# --- 3. データの取得 ---
@st.cache_data
def load_data():
    client = bigquery.Client()
    
    # 修正後のSQLクエリ
    query = """
        SELECT 
            Date, Ticker, Open, Close, High, Low, Volume, SMA25, SMA75, SMA200
        FROM `stock-data-updater-490714.stock_db.daily_prices`
        ORDER BY Date
    """
    df = client.query(query).to_dataframe()
    # Date型への変換
    df['Date'] = pd.to_datetime(df['Date'])
    return df

# データの読み込み
df = load_data()

st.title("📊 株価分析ダッシュボード (Streamlit版)")

# --- 4. 画面のUI（サイドバー） ---
st.sidebar.header("検索条件")

# 銘柄のプルダウン (Ticker)
tickers = df['Ticker'].unique()
selected_ticker = st.sidebar.selectbox("銘柄を選択", tickers)

# 選択した銘柄でフィルタリング
filtered_df = df[df['Ticker'] == selected_ticker]

# 日付のスライダー
min_date = filtered_df['Date'].min()
max_date = filtered_df['Date'].max()

# データが空でないかチェック
if not filtered_df.empty:
    start_date, end_date = st.sidebar.slider(
        "期間を選択",
        min_value=min_date.date(),
        max_value=max_date.date(),
        value=(min_date.date(), max_date.date())
    )

    # 日付でさらに絞り込み
    mask = (filtered_df['Date'].dt.date >= start_date) & (filtered_df['Date'].dt.date <= end_date)
    final_df = filtered_df.loc[mask].sort_values('Date')

    # --- 5. Plotlyでローソク足＆移動平均線を描画 ---
    fig = go.Figure()

    # ① ローソク足 (Open, Close, High, Low)
    fig.add_trace(go.Candlestick(
        x=final_df['Date'],
        open=final_df['Open'],
        high=final_df['High'],
        low=final_df['Low'],
        close=final_df['Close'],
        name='ローソク足'
    ))

    # ② 移動平均線 (SMA25, SMA75, SMA200)
    fig.add_trace(go.Scatter(x=final_df['Date'], y=final_df['SMA25'], mode='lines', name='25日線', line=dict(color='orange', width=1.5)))
    fig.add_trace(go.Scatter(x=final_df['Date'], y=final_df['SMA75'], mode='lines', name='75日線', line=dict(color='green', width=1.5)))
    fig.add_trace(go.Scatter(x=final_df['Date'], y=final_df['SMA200'], mode='lines', name='200日線', line=dict(color='blue', width=1.5)))

    # レイアウトの調整
    fig.update_layout(
        title=f"【{selected_ticker}】 日足チャート",
        yaxis_title="価格 (円)",
        xaxis_rangeslider_visible=False,
        height=700,
        hovermode="x unified",
        template="plotly_white" # 白背景で見やすく
    )

    # --- 6. 出来高グラフを下に表示 ---
    vol_fig = go.Figure()
    vol_fig.add_trace(go.Bar(x=final_df['Date'], y=final_df['Volume'], name='出来高', marker_color='lightgrey'))
    vol_fig.update_layout(height=200, title="出来高", margin=dict(t=30, b=0))

    # 画面に表示
    st.plotly_chart(fig, use_container_width=True)
    st.plotly_chart(vol_fig, use_container_width=True)
else:
    st.error("データが見つかりませんでした。")