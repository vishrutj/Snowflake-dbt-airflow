import pandas as pd
from datetime import datetime
import uuid
import logging
from utils.snowflake_connector import SnowflakeConnector
from snowflake.connector.pandas_tools import write_pandas
import os

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)

# Get the absolute path to the project root
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

def load_to_snowflake(df: pd.DataFrame, batch_id: str) -> None:
    """
    Load Dataframe to Snowflake Bronze Layer

    Args:
    df -> Dataframe to be loaded
    batch_id -> Unique Identifier for this load
    """

    try:
        #Reset index to make a date column

        df = df.reset_index()

        columns_to_drop = ['index', 'Dividends', 'Stock Splits']
        df = df.drop(columns=columns_to_drop, errors='ignore')

        #Rename columns to match snowflake schema

        df = df.rename(columns={
            'Date' : 'TRADING_DATE',
            'Open' : 'OPEN_PRICE',
            'High' : 'HIGH_PRICE',
            'Low' : 'LOW_PRICE',
            'Close': 'CLOSE_PRICE',
            'Volume': 'VOLUME',
            'Adj Close': 'ADJUSTED_CLOSE',
            'ticker': 'TICKER',
            'download_timestamp': 'DOWNLOAD_TIMESTAMP'
        })


       # Add metadata columns
        current_timestamp = datetime.now()
        df['FILE_NAME'] = f'stock_prices_{current_timestamp.strftime("%Y%m%d_%H%M%S")}.csv'
        df['LOAD_TIMESTAMP'] = current_timestamp
        df['SOURCE_SYSTEM'] = 'YAHOO_FINANCE'
        df['BATCH_ID'] = batch_id
        #Initialize Snowflake Connection
        sf = SnowflakeConnector()

        conn = sf.get_connection()

        #Load to Snowflake
        success, nchunks, nrows, _ = write_pandas(
            conn=conn,
            df=df,
            table_name='STOCK_PRICES',
            database='FINANCE_DB',
            schema='RAW'
        )

        logger.info(f"Loaded {nrows} rows to Snowflake in {nchunks} chunks")
        
        # Close connection
        sf.close_connection()
        
    except Exception as e:
        logger.error(f"Failed to load data to Snowflake: {str(e)}")
        raise

def main():
    try:
        csv_path = os.path.join(PROJECT_ROOT, 'data', 'raw_stock_data.csv')
        
        # Log the path we're trying to read from
        logger.info(f"Attempting to read CSV from: {csv_path}")
        
        # Verify file exists
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"CSV file not found at: {csv_path}")

        df = pd.read_csv(csv_path)
        logger.info(f"Successfully read CSV with {len(df)} rows")
        # Generate batch ID
        batch_id = str(uuid.uuid4())
        
        # Load to Snowflake
        load_to_snowflake(df, batch_id)
        
    except Exception as e:
        logger.error(f"Failed to process data: {str(e)}")
        raise

if __name__ == "__main__":
    main()