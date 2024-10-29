from snowflake.connector import connect
from snowflake.connector.pandas_tools import write_pandas
import os
from dotenv import load_dotenv
import logging

logger = logging.getLogger(__name__)

class SnowflakeConnector:
    def __init__(self):
        load_dotenv()
        self.conn = None
        
    def get_connection(self):
        """Create Snowflake connection"""
        try:
            # Log connection attempt (without sensitive info)
            logger.info(f"Attempting to connect to Snowflake")
            logger.info(f"Account: {os.getenv('SNOWFLAKE_ACCOUNT')}")
            logger.info(f"Warehouse: {os.getenv('SNOWFLAKE_WAREHOUSE')}")
            logger.info(f"Database: {os.getenv('SNOWFLAKE_DATABASE')}")
            
            if not self.conn:
                self.conn = connect(
                    user=os.getenv('SNOWFLAKE_USER'),
                    password=os.getenv('SNOWFLAKE_PASSWORD'),
                    account=os.getenv('SNOWFLAKE_ACCOUNT'),
                    warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
                    database=os.getenv('SNOWFLAKE_DATABASE'),
                    schema=os.getenv('SNOWFLAKE_SCHEMA')
                )
                logger.info("Successfully connected to Snowflake")
            return self.conn
            
        except Exception as e:
            logger.error(f"Failed to connect to Snowflake")
            logger.error(f"Error details: {str(e)}")
            logger.error("Please verify your Snowflake credentials and account information")
            raise
    
    def execute_query(self, query: str, is_ddl: bool = True):
        """
        Execute a query
        Args:
            query (str): SQL query to execute
            is_ddl (bool): True if DDL statement (CREATE, ALTER, etc.), False if query returns results
        """
        try:
            if not self.conn:
                self.conn = self.get_connection()
                
            cursor = self.conn.cursor()
            
            # Log the query (first 200 chars)
            logger.info(f"Executing query: {query[:200]}...")
            
            cursor.execute(query)
            
            if is_ddl:
                # For DDL statements, just execute
                cursor.close()
                return None
            else:
                # For queries that return results
                results = cursor.fetchall()
                cursor.close()
                return results
                
        except Exception as e:
            logger.error(f"Query execution failed: {str(e)}")
            logger.error(f"Failed query: {query}")
            raise
    
    def test_connection(self):
        """Test Snowflake connection and verify access"""
        try:
            if not self.conn:
                self.conn = self.get_connection()
                
            cursor = self.conn.cursor()
            
            # Test query to verify access
            cursor.execute('SELECT CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_SCHEMA()')
            result = cursor.fetchone()
            
            logger.info("Connection Test Results:")
            logger.info(f"Current Warehouse: {result[0]}")
            logger.info(f"Current Database: {result[1]}")
            logger.info(f"Current Schema: {result[2]}")
            
            cursor.close()
            return True
            
        except Exception as e:
            logger.error("Connection test failed")
            logger.error(f"Error: {str(e)}")
            return False
            
    def close_connection(self):
        """Close Snowflake connection"""
        if self.conn:
            self.conn.close()
            self.conn = None
            logger.info("Snowflake connection closed")