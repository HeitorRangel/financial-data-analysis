import os
import logging
import datetime

import pandas as pd
import plotly.express as px
import streamlit as st
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration from .env
DATA_PATH = os.getenv("BASE_PATH", "datalake")

# Logging setup for Streamlit
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Page Configuration
st.set_page_config(
    page_title="Financial Data Lake",
    layout="wide",
    initial_sidebar_state="expanded"
)

def classificar_ativo(ticker):
    """Classifica o tipo de ativo baseado no sufixo do código B3."""
    if ticker.endswith('34') or ticker.endswith('35'):
        return 'BDR (Internacional)'
    elif ticker.endswith('11'):
        return 'FII / ETF'
    elif ticker.endswith('3') or ticker.endswith('4') or ticker.endswith('5') or ticker.endswith('6'):
        return 'Ação (Nacional)'
    else:
        return 'Outros'

@st.cache_data(ttl=60)
def load_data(path: str) -> pd.DataFrame:
    """
    Loads data from the Parquet Data Lake.
    Uses caching to improve performance.
    """
    try:
        if not os.path.exists(path):
            logger.warning(f"Data Lake path not found: {path}")
            return pd.DataFrame()
            
        df = pd.read_parquet(path, engine='fastparquet')
        
        # Ensure Datetime is datetime type
        if 'Datetime' in df.columns:
            df['Datetime'] = pd.to_datetime(df['Datetime'])
            
        # Add Asset Classification
        if 'Ticker' in df.columns:
            df['Tipo de Ativo'] = df['Ticker'].apply(classificar_ativo)
        
        return df
    except Exception as e:
        logger.error(f"Error loading data: {e}")
        st.error(f"Error loading data: {e}")
        return pd.DataFrame()

def main():
    st.title("Financial Market Data Analysis")

    if not os.path.exists(DATA_PATH):
        st.warning("Data Lake directory not found. Please wait for the ingestor script to generate data.")
        return

    df = load_data(DATA_PATH)

    if df.empty:
        st.info("No data available in the Data Lake yet.")
        return

    # Sidebar Filters
    st.sidebar.header("Filters")
    
    # Asset Categorization Filter
    if 'Tipo de Ativo' in df.columns:
        tipos_disponiveis = sorted(df['Tipo de Ativo'].unique())
        tipo_selecionado = st.sidebar.multiselect(
            "Select Asset Type", 
            tipos_disponiveis, 
            default=tipos_disponiveis
        )
        # Filter dataframe base
        df_filtrado_tipo = df[df['Tipo de Ativo'].isin(tipo_selecionado)]
    else:
        df_filtrado_tipo = df

    # Asset Filter (now dependent on the type selected above)
    if not df_filtrado_tipo.empty:
        available_assets = sorted(df_filtrado_tipo['Ticker'].unique())
    else:
        available_assets = []
        
    selected_asset = st.sidebar.selectbox("Select Asset", available_assets)

    # Date Filter
    min_date = df['Datetime'].min().date()
    max_date = df['Datetime'].max().date()
    
    date_range = st.sidebar.date_input(
        "Select Date Range",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date
    )

    if isinstance(date_range, tuple) and len(date_range) == 2:
        start_date, end_date = date_range
    else:
        start_date = end_date = date_range[0] if isinstance(date_range, tuple) else date_range

    # Filter Data
    filtered_df = df[
        (df['Ticker'] == selected_asset) & 
        (df['Datetime'].dt.date >= start_date) &
        (df['Datetime'].dt.date <= end_date)
    ].sort_values('Datetime')

    if filtered_df.empty:
        st.warning(f"No data found for {selected_asset} in the selected period.")
        return

    # Calculate Moving Average (20 periods)
    filtered_df['SMA 20'] = filtered_df['Close'].rolling(window=20).mean()

    # KPIs
    st.header(f"Overview: {selected_asset}")
    
    col1, col2, col3 = st.columns(3)
    
    current_price = filtered_df['Close'].iloc[-1]
    start_price = filtered_df['Close'].iloc[0]
    variation = ((current_price - start_price) / start_price) * 100
    volume_records = len(filtered_df)

    col1.metric("Last Price", f"R$ {current_price:.2f}")
    col2.metric("Variation (Day)", f"{variation:.2f}%")
    col3.metric("Data Points", f"{volume_records}")

    # Charts
    st.subheader("Price Evolution")
    
    fig = px.line(
        filtered_df, 
        x='Datetime', 
        y=['Close', 'SMA 20'], 
        title=f'{selected_asset} - Historical Price',
        template='plotly_white',
        color_discrete_map={'Close': '#1f77b4', 'SMA 20': '#ff7f0e'}
    )
    
    fig.update_layout(
        xaxis_title="Time",
        yaxis_title="Price (BRL)",
        hovermode="x unified",
        legend_title_text="Indicator"
    )
    
    st.plotly_chart(fig)

    # Data Table
    st.subheader("Raw Data")
    
    st.dataframe(
        filtered_df[['Datetime', 'Ticker', 'Close']].sort_values('Datetime', ascending=False),
        hide_index=True
    )

if __name__ == "__main__":
    main()
