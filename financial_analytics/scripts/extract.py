#Data Processing Libraries
import yfinance as yf
from pyspark.sql import SparkSession
import pandas as pd

#Environment Libraries
import os
from dotenv import load_dotenv
import yaml #for config files

#Logging and Error Handling
import logging
from datetime import datetime

#Setting up Logging
logging.basicConfig(
    level = logging.INFO,
    format = '%(asctime)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

#Create a Spark Session

# def create_spark_session():
#     """
#     Create a spark session with specified configuration. Returns a configured spark session
#     """

#     try:
#         spark = (SparkSession.builder
#         .appName('finance pipeline') #Identifies your application in Spark UI
#         .master('local[*]') #Use All Available Cores  * - All Cores, 1 - 1 core, n - n cores
#         .config('spark.driver.memory','4g') #Driver Memory - Controls memory allocation
#         .config('spark.sql.shuffle.partitions','4') #For Local Development
#         .config('spark.default.parallelism','4') #partitions: Affects performance and parallelism
#         .config('spark.sql.session.timezone','UTC') #timeZone: Ensures consistent timestamp handling
#         .getOrCreate() # Reuses existing session if available, More efficient resource usage, Prevents multiple session creation
#         )
    
#         logger.info("Spark Session Created Successfully")

#         return spark
    
#     except Exception as e:
#         logger.error(f"Failed to create Spark Session: {str(e)}")
#         raise

def load_config():
    """
    Load Configuration from YAML file:
    Returns:
        dict: Configuration Parameter
    """
    try:
        print(os.path.dirname(__file__))
        config_path = os.path.join(os.path.dirname(__file__),'../config/config.yaml')
        with open(config_path,'r') as file:
            config = yaml.safe_load(file)
            logger.info("Configuration Loaded Successfully")
            return config

    except Exception as e:
        logger.error(f"Failed to Load Configuration: {str(e)}")

        raise

def extract_stock_data(ticker: str,start_date: str, end_date: str) -> pd.DataFrame:
    """
    Extract data for a given ticker and date range

    Args: 
    ticker: Ticker symbol for the stock
    start date(str): Start Date in YYYY-MM-DD format
    end date(str): End Date in YYYY-MM-DD format

    Returns a pd.DataFrame  

    Raises an error if the Stock Ticker doesn;t exist or dates are invalid

    """
    logger.info("Extracting data for {ticker} from {start_date} to {end_date}")

    try:
        if not ticker or not isinstance(ticker, str):
            raise ValueError("Invalid Ticker Symbol")

        #Create a ticker object
        stock = yf.Ticker(ticker)

        #Fetch Data
        stock_data = stock.history(
            start = start_date,
            end = end_date,
            interval = '1d' #Daily Data
        )

        stock_data['ticker'] = ticker

        stock_data['download_timestamp'] = datetime.now()

        logger.info(f'Successfully extraction {len(stock_data)} records for {ticker}')

        return stock_data

    except ValueError as ve:
        logger.error(f"Validation error for stock {ticker}: {str(ve)}")
        raise

    except Exception as e:
        logger.error(f"Exception Error: {str(e)}")
        raise

#Function to convert this to Spark DataFrame

# def convert_to_spark_df(spark: SparkSession, pandas_df: pd.DataFrame, ticket: str) -> None:
#     """
#     Convert Pandas Dataframe to Spark Dataframe
#     Args:
#     spark: Active Spark Session
#     pandas_df: Stock Data Dataframe that needs to be converted to a Spark Dataframe
#     ticker: Stock Ticker Symbol

#     Returns:
#     pyspark.sql.Dataframe: Transformed Spark Dataframe

#     """
        
#     try:
#         if pandas_df is None or pandas_df.empty:
#             logger.warning(f"No Data to convert for ticker {ticker}")
#             return None


#         spark_df = spark.createDataFrame(pandas_df.reset_index())

#         #Perform Basic Transformation

#         spark_df = spark_df.selectExpr(
#             "Date as trading_date",
#             "Open as open_price",
#             "High as high_price",
#             "Low as low_price",
#             "Close as close_price",
#             "Volume as volume",
#             "ticker",
#             "download_timestamp"
#         )

#         logger.info(f"Successfully converted data for {ticker} to Spark DataFrame")

#         return spark_df


#     except Exception as e:
#         logger.error(f"Failed to convert data for {ticker} to spark dataframe: {str(e)}")
#         raise


def main():
    "Main function to orchestrate the data extraction process"

    #Load Configuration

    config = load_config()
    tickers = config.get('tickers',['MSFT','AAPL','GOOGL','META','SMCI','TSLA','NFLX'])

    #Initialize Spark

    # spark = create_spark_session()

    all_stock_data = []

    for ticker in tickers:
        try:
            #Extract Data
            pandas_df = extract_stock_data(
                ticker = ticker,
                start_date = config['start_date'],
                end_date = config['end_date']
            )

            #Convert to Spark Dataframe

            # spark_df = convert_to_spark_df(spark, pandas_df, ticker)

            # if spark_df is not None:
            #     all_stock_data.append(spark_df)

            if pandas_df is not None:
                all_stock_data.append(pandas_df)

        except Exception as e:
            logger.error(f"Failed Processing {ticker}: {str(e)}")
            continue


    if all_stock_data:
        final_df = pd.concat(all_stock_data)


        #Show sample Data
        logger.info("Sample of extracted Data:")

        final_df.head(5)

        # TODO: Add data quality checks
        # TODO: Save to temporary location for next step
        output_path = os.path.join(os.path.dirname(__file__), '../data/raw_stock_data.csv')
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        final_df.to_csv(output_path)
        logger.info(f"Data saved to {output_path}")
    

if __name__ == "__main__":
    main()