import logging
from utils.snowflake_connector import SnowflakeConnector
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def execute_sql_file(connector: SnowflakeConnector, sql_file_path: str):
    """Execute SQL commands from a file"""
    try:
        # Read the SQL file
        with open(sql_file_path, 'r') as file:
            sql_content = file.read()
            
        # Clean and split commands properly
        commands = []
        current_command = []
        
        # Split by lines to handle multi-line commands
        for line in sql_content.split('\n'):
            line = line.strip()
            if line and not line.startswith('--'):  # Skip empty lines and comments
                current_command.append(line)
                if line.endswith(';'):
                    # Join the command lines and add to commands list
                    commands.append(' '.join(current_command))
                    current_command = []
        
        logger.info(f"Total commands to execute: {len(commands)}")
        
        # Execute each command
        for i, command in enumerate(commands, 1):
            if command.strip():
                logger.info(f"\nExecuting command {i}:")
                logger.info(f"Command content:\n{command}")
                try:
                    connector.execute_query(command)
                    logger.info(f"Successfully executed command {i}")
                except Exception as e:
                    logger.error(f"Error executing command {i}: {str(e)}")
                    raise
                
    except Exception as e:
        logger.error(f"Failed to execute SQL file: {str(e)}")
        raise

def main():
    try:
        # Initialize Snowflake connector
        sf = SnowflakeConnector()
        
        # Get path to SQL file
        sql_file_path = os.path.join(
            os.path.dirname(__file__), 
            'sql/create_bronze_schema.sql'
        )
        
        # Verify file exists
        if not os.path.exists(sql_file_path):
            raise FileNotFoundError(f"SQL file not found at: {sql_file_path}")
            
        logger.info(f"Found SQL file at: {sql_file_path}")
        
        # Execute setup SQL
        execute_sql_file(sf, sql_file_path)
        logger.info("Bronze schema setup completed successfully!")
        
    except Exception as e:
        logger.error(f"Setup failed: {str(e)}")
    finally:
        sf.close_connection()

if __name__ == "__main__":
    main()