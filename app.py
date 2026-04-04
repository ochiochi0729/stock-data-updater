import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import plotly.graph_objects as go

# --- 1. ページの設定 ---
st.set_page_config(page_title="株価分析アプリ", layout="wide")

# --- 2. 認証設定 ---
if "gcp_service_account" in st.secrets:
    creds_info = st.secrets["gcp_service_account"]
    credentials = service_account.Credentials.from_service_account_info(creds_info)
    client = bigquery.Client(credentials=credentials, project=creds_info['project_id'])
else:
    st.error("Secretsが設定されていません。")
    st.stop()

# --- 3. 銘柄リストだけを先に取得する関数 ---
@st.cache_data
def get_ticker_list():
    # 銘柄コード（Ticker）の重複を除いたリストだけを取得（これは軽い）
    query = "SELECT DISTINCT Ticker FROM `stock-data-updater-490714.stock_db.daily_prices` ORDER BY Ticker"
    return client.query(query).to_dataframe()['Ticker'].tolist()

# --- 4. 選択された銘柄のデータだけを取得する関数 ---
@st.cache_data
def load_data_by_ticker(ticker):
    # 特定の銘柄（WHERE句）で絞り込んで取得
    query = f"""
        SELECT 
            Date, Ticker, Open, Close, High, Low, Volume, SMA25, SMA75, SMA200
        FROM `stock-data-updater-490714.stock_db.daily_prices`
        WHERE Ticker = '{ticker}'
        ORDER BY Date
    """
    df = client.query(query).to_dataframe()
    df['Date'] = pd.to_datetime(df['Date'])
    return df

# --- 5. メイン処理 ---
st.title("📊 株価分析ダッシュボード")

# サイドバーで銘柄を選択
ticker_list = get_ticker_list()
selected_ticker = st.sidebar.selectbox("銘柄を選択してください", ticker_list)

if selected_ticker:
    # 選択された銘柄のデータだけをダウンロード
    with st.spinner(f'{selected_ticker} のデータを読み込み中...'):
        df = load_data_by_ticker(selected_ticker)

    if not df.empty:
        # 日付範囲の選択
        min_date = df['Date'].min().date()
        max_date = df['Date'].max().date()
        start_date, end_date = st.sidebar.slider("期間", min_date, max_date, (min_date, max_date))

        # データのフィルタリング
        final_df = df[(df['Date'].dt.date >= start_date) & (df['Date'].dt.date <= end_date)]

        # チャート作成
        fig = go.Figure()
        fig.add_trace(go.Candlestick(
            x=final_df['Date'], open=final_df['Open'], high=final_df['High'],
            low=final_df['Low'], close=final_df['Close'], name='ローソク足'
        ))
        # 移動平均線
        for sma, color in zip(['SMA25', 'SMA75', 'SMA200'], ['orange', 'green', 'blue']):
            fig.add_trace(go.Scatter(x=final_df['Date'], y=final_df[sma], name=sma, line=dict(color=color, width=1.5)))

        fig.update_layout(height=600, xaxis_rangeslider_visible=False, template="plotly_white")
        st.plotly_chart(fig, use_container_width=True)

        # 出来高
        vol_fig = go.Figure(go.Bar(x=final_df['Date'], y=final_df['Volume'], marker_color='silver'))
        vol_fig.update_layout(height=200, margin=dict(t=0))
        st.plotly_chart(vol_fig, use_container_width=True)
    else:
        st.warning("データが存在しません。")
