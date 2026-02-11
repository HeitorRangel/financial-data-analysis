import datetime
import os

import pandas as pd
import plotly.express as px
import streamlit as st

# Page Configuration
st.set_page_config(
    page_title="Financial Data Lake",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Constants
DATA_PATH = "datalake"

@st.cache_data(ttl=60)
def load_data(path: str) -> pd.DataFrame:
    """
    Loads data from the Parquet Data Lake.
    Uses caching to improve performance.
    """
    try:
        # Load the dataset using pyarrow engine for better performance with partitioned data
        if not os.path.exists(path):
            return pd.DataFrame()
            
        df = pd.read_parquet(path, engine='fastparquet')
        
        # Ensure Datetime is datetime type
        if 'Datetime' in df.columns:
            df['Datetime'] = pd.to_datetime(df['Datetime'])
        
        return df
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return pd.DataFrame()

def main():
    st.title("Financial Market Data Analysis")

    # Load Data
    import os
    if not os.path.exists(DATA_PATH):
        st.warning("Data Lake directory not found. Please run the ingestor script first.")
        return

    df = load_data(DATA_PATH)

    if df.empty:
        st.info("No data available in the Data Lake yet.")
        return

    # Sidebar Filters
    st.sidebar.header("Filters")
    
    # Asset Filter
    available_assets = sorted(df['Ticker'].unique())
    selected_asset = st.sidebar.selectbox("Select Asset", available_assets)

    # Date Filter
    min_date = df['Datetime'].min().date()
    max_date = df['Datetime'].max().date()
    
    selected_date = st.sidebar.date_input(
        "Select Date",
        value=max_date,
        min_value=min_date,
        max_value=max_date
    )

    # Filter Data
    filtered_df = df[
        (df['Ticker'] == selected_asset) & 
        (df['Datetime'].dt.date == selected_date)
    ].sort_values('Datetime')

    if filtered_df.empty:
        st.warning(f"No data found for {selected_asset} on {selected_date}.")
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
        title=f'{selected_asset} - Intraday Price',
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
