import time
import os
import datetime
import logging
from typing import List, Optional

import pandas as pd
import requests
import yfinance as yf
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration from .env
ASSETS_ENV_STR = os.getenv("ASSETS", "")
ASSETS: List[str] = [asset.strip() for asset in ASSETS_ENV_STR.split(",")] if ASSETS_ENV_STR else []
INTERVAL_SECONDS: int = int(os.getenv("INTERVAL_SECONDS", "120"))
BASE_PATH: str = os.getenv("BASE_PATH", "datalake")

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
    if not assets:
        logger.warning("No assets defined for ingestion. Check your .env file.")
        return pd.DataFrame()

    logger.info("Iniciando coleta...")
    
    # CRIANDO UMA SESSÃO DISFARÇADA DE NAVEGADOR (CHROME)
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
    })

    try:
        # Passamos a "session" como parâmetro extra
        data = yf.download(assets, period='1d', interval='1m', progress=False, session=session)
        
        if data.empty:
            logger.warning("No data returned from API.")
            return pd.DataFrame()

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
        df.index.name = 'Datetime'
        df_reset = df.reset_index()

        if 'Datetime' not in df_reset.columns:
            logger.error("Datetime column missing after reset_index.")
            return pd.DataFrame()

        df_melted = df_reset.melt(
            id_vars=['Datetime'], 
            var_name='Ticker', 
            value_name='Close'
        )

        df_melted = df_melted.dropna(subset=['Close'])

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
        os.makedirs(base_path, exist_ok=True)
        
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
