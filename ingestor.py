import time
import os
import datetime
import logging
from typing import List, Optional

import pandas as pd
import yfinance as yf

# Configuration
ASSETS: List[str] = [
    'HGLG11.SA', 'KNRI11.SA', 'MXRF11.SA', 'XPML11.SA', 'VISC11.SA',
    'ALZR11.SA', 'HGRU11.SA', 'BTLG11.SA', 'XPLG11.SA', 
    'CPTS11.SA', 'RECR11.SA', 'VGHF11.SA', 'KNCR11.SA', 
    'PETR4.SA', 'VALE3.SA', 'ITUB4.SA', 'BBDC4.SA', 'BBAS3.SA',
    'WEGE3.SA', 'ABEV3.SA', 'B3SA3.SA', 'RENT3.SA',
    'SUZB3.SA', 'GGBR4.SA', 'VIVT3.SA', 'PRIO3.SA',
    'BOVA11.SA', 'IVVB11.SA', 'SMAL11.SA', 'HASH11.SA', 'NASD11.SA',
    'XINA11.SA', 'GOLD11.SA', 'AAPL34.SA', 'MSFT34.SA',
    'NVDC34.SA', 'AMZO34.SA', 'GOGL34.SA', 'TSLA34.SA', 'MELI34.SA'
]
INTERVAL_SECONDS: int = 120  # 2 minutes
BASE_PATH: str = "datalake"

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def fetch_data(assets: List[str]) -> pd.DataFrame:
    """
    Fetches the latest market data for the given assets.
    """
    try:
        # Download data for all assets at once
        data = yf.download(assets, period='1d', interval='1m', progress=False)
        
        if data.empty:
            logger.warning("No data returned from API.")
            return pd.DataFrame()

        # Extract Close prices
        try:
            df_close = data['Close']
        except KeyError:
            logger.error("Column 'Close' not found in API response.")
            return pd.DataFrame()
            
        return df_close

    except Exception as e:
        logger.error(f"Error fetching data: {e}")
        return pd.DataFrame()

def process_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms the data into Tidy (Long) format and adds partition columns.
    """
    if df.empty:
        return pd.DataFrame()

    try:
        # Ensure index name for resetting
        df.index.name = 'Datetime'
        df_reset = df.reset_index()

        if 'Datetime' not in df_reset.columns:
            logger.error("Datetime column missing after reset_index.")
            return pd.DataFrame()

        # Melt DataFrame to Tidy format
        df_melted = df_reset.melt(
            id_vars=['Datetime'], 
            var_name='Ticker', 
            value_name='Close'
        )

        # Drop rows with NaN values (failed downloads)
        df_melted = df_melted.dropna(subset=['Close'])

        # Add partition columns
        df_melted['ano'] = df_melted['Datetime'].dt.year
        df_melted['mes'] = df_melted['Datetime'].dt.month
        df_melted['dia'] = df_melted['Datetime'].dt.day
        
        return df_melted

    except Exception as e:
        logger.error(f"Error processing data: {e}")
        return pd.DataFrame()

def save_to_datalake(df: pd.DataFrame, base_path: str) -> None:
    """
    Saves the DataFrame to the Data Lake using Parquet format with Snappy compression.
    """
    if df.empty:
        return

    try:
        current_time = datetime.datetime.now()
        filename = f"market_data_{current_time.strftime('%H%M%S')}.parquet"
        
        # Ensure the directory exists is handled by partition_cols, 
        # but to_parquet might need the base directory locally if completely empty, 
        # usually fastparquet handles it.
        
        df.to_parquet(
            path=base_path,
            engine='fastparquet',
            partition_cols=['ano', 'mes', 'dia'],
            compression='snappy',
            index=False
        )
        
        logger.info(f"Data saved successfully to {base_path}")

    except Exception as e:
        logger.error(f"Error saving to Data Lake: {e}")

def main():
    logger.info("Starting Financial Data Ingestion Service...")
    
    while True:
        try:
            logger.info("Fetching market data...")
            df_raw = fetch_data(ASSETS)
            
            if not df_raw.empty:
                df_processed = process_data(df_raw)
                
                if not df_processed.empty:
                    save_to_datalake(df_processed, BASE_PATH)
                else:
                    logger.warning("Processed data is empty.")
            
            logger.info(f"Sleeping for {INTERVAL_SECONDS} seconds...")
            time.sleep(INTERVAL_SECONDS)

        except KeyboardInterrupt:
            logger.info("Service stopped by user.")
            break
        except Exception as e:
            logger.critical(f"Critical error in main loop: {e}")
            time.sleep(60)

if __name__ == "__main__":
    main()
