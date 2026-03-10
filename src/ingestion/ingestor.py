import time
import os
import datetime
import logging
from typing import List, Optional

import pandas as pd
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration from .env
ASSETS_ENV_STR = os.getenv("ASSETS", "")
ASSETS: List[str] = [asset.strip() for asset in ASSETS_ENV_STR.split(",")] if ASSETS_ENV_STR else []
INTERVAL_SECONDS: int = int(os.getenv("INTERVAL_SECONDS", "3600"))
BASE_PATH: str = os.getenv("BASE_PATH", "datalake")

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def is_market_open() -> bool:
    """
    Verifica se a B3 (Bolsa de Valores do Brasil) está aberta.
    Considera dias úteis (segunda a sexta) e horário das 10h às 18h (Brasília).
    """
    now = pd.Timestamp.now('America/Sao_Paulo')
    
    # Verifica se é fim de semana (5 = Sábado, 6 = Domingo)
    if now.weekday() >= 5:
        return False
        
    # Verifica se está no horário comercial (10h às 18h)
    if 10 <= now.hour < 18:
        return True
        
    return False

def fetch_data(assets: List[str]) -> pd.DataFrame:
    """
    Fetches the latest market data for the given assets using Brapi API.
    """
    if not assets:
        logger.warning("No assets defined for ingestion. Check your .env file.")
        return pd.DataFrame()

    logger.info("Iniciando coleta via Brapi...")
    
    brapi_assets = [asset.replace('.SA', '') for asset in assets]
    
    # CRIANDO UMA SESSÃO DISFARÇADA DE NAVEGADOR (CHROME)
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
    })
    
    token = os.getenv("BRAPI_TOKEN", "")
    tamanho_lote = 1 # Restrição da API Brapi Free (1 ativo por requisição)
    records = []

    for i in range(0, len(brapi_assets), tamanho_lote):
        lote_atual = brapi_assets[i:i + tamanho_lote]
        tickers_str = ','.join(lote_atual)
        url = f"https://brapi.dev/api/quote/{tickers_str}?range=1d&interval=1m"
        
        if token:
            url += f"&token={token}"

        try:
            response = session.get(url, timeout=15)
            
            if response.status_code == 200:
                data = response.json()
                
                if 'error' in data:
                    logger.error(f"Brapi returned an error for batch {tickers_str}: {data['error']}")
                    continue

                if 'results' in data:
                    for result in data['results']:
                        # The symbol from Brapi is without .SA, let's find the original to maintain compatibility
                        symbol = result.get('symbol', '')
                        original_asset = next((a for a in assets if a.replace('.SA', '') == symbol), symbol)
                        
                        historical_data = result.get('historicalDataPrice', [])
                        for item in historical_data:
                            records.append({
                                'Datetime': pd.to_datetime(item['date'], unit='s', utc=True).tz_convert('America/Sao_Paulo').tz_localize(None),
                                'Ticker': original_asset,
                                'Close': item['close']
                            })
            else:
                logger.error(f"Error in batch {tickers_str}: HTTP {response.status_code}")
                
        except Exception as e:
            logger.error(f"Connection failure fetching batch {tickers_str}: {e}")
            
        # Rate Limiting: Pause between batches to avoid spam blocking
        time.sleep(2)

    df = pd.DataFrame(records)
    if df.empty:
        logger.warning("No historical data returned from Brapi API.")
        return pd.DataFrame()
        
    # Transform the dataframe to have 'Datetime' as index and 'Ticker' as columns, matching the previous yfinance structure
    df_pivot = df.pivot_table(index='Datetime', columns='Ticker', values='Close')
    return df_pivot

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
            if not is_market_open():
                logger.info("Mercado fechado (fora do horário comercial ou fim de semana). Aguardando próximo ciclo...")
                time.sleep(INTERVAL_SECONDS)
                continue

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
