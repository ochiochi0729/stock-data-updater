import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import plotly.graph_objects as go

# --- 1. ページの設定 ---
st.set_page_config(page_title="株価ダッシュボード", layout="wide")

# --- 2. BigQueryの認証設定（クラウドSecrets用） ---
# Streamlitの「Advanced settings」>「Secrets」に保存した情報を読み込みます
if "gcp_service_account" in st.secrets:
    creds_info = st.secrets["gcp_service_account"]
    credentials = service_account.Credentials.from_service_account_info(creds_info)
else:
    st.error("Secretsが設定されていません。Streamlitの管理画面から鍵を登録してください。")
    st.stop()

# --- 3. データの取得 ---
@st.cache_data
def load_data():
    # 認証情報(credentials)を使ってクライアントを作成
    client = bigquery.Client(credentials=credentials, project=creds_info['project_id'])
    
    query = """
        SELECT 
            Date, Ticker, Open, Close, High, Low, Volume, SMA25, SMA75, SMA200
        FROM `stock-data-updater-490714.stock_db.daily_prices`
        ORDER BY Date
    """
    df = client.query(query).to_dataframe()
    df['Date'] = pd.to_datetime(df['Date'])
    return df

# データの読み込み
try:
    df = load_data()
except Exception as e:
    st.error(f"データの読み込み中にエラーが発生しました: {e}")
    st.stop()

st.title("📊 株価分析ダッシュボード")

# --- 4. 画面のUI（サイドバー） ---
st.sidebar.header("検索条件")
tickers = df['Ticker'].unique()
selected_ticker = st.sidebar.selectbox("銘柄を選択", tickers)

filtered_df = df[df['Ticker'] == selected_ticker]

if not filtered_df.empty:
    min_date = filtered_df['Date'].min().date()
    max_date = filtered_df['Date'].max().date()

    start_date, end_date = st.sidebar.slider(
        "期間を選択",
        min_value=min_date,
        max_value=max_date,
        value=(min_date, max_date)
    )

    mask = (filtered_df['Date'].dt.date >= start_date) & (filtered_df['Date'].dt.date <= end_date)
    final_df = filtered_df.loc[mask].sort_values('Date')

    # --- 5. チャート描画 ---
    fig = go.Figure()
    fig.add_trace(go.Candlestick(
        x=final_df['Date'], open=final_df['Open'], high=final_df['High'],
        low=final_df['Low'], close=final_df['Close'], name='ローソク足'
    ))
    fig.add_trace(go.Scatter(x=final_df['Date'], y=final_df['SMA25'], mode='lines', name='25日線', line=dict(color='orange')))
    fig.add_trace(go.Scatter(x=final_df['Date'], y=final_df['SMA75'], mode='lines', name='75日線', line=dict(color='green')))
    fig.add_trace(go.Scatter(x=final_df['Date'], y=final_df['SMA200'], mode='lines', name='200日線', line=dict(color='blue')))

    fig.update_layout(height=600, template="plotly_white", xaxis_rangeslider_visible=False)
    st.plotly_chart(fig, use_container_width=True)

    # 出来高
    vol_fig = go.Figure(go.Bar(x=final_df['Date'], y=final_df['Volume'], marker_color='lightgrey'))
    vol_fig.update_layout(height=200, margin=dict(t=0))
    st.plotly_chart(vol_fig, use_container_width=True)
