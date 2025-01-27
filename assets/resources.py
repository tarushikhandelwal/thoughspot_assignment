from dagster import IOManager, io_manager
import sqlite3
import pandas as pd
from typing import Any

class SQLiteIOManager(IOManager):
    def __init__(self, database_uri: str):
        self.database_uri = database_uri

    def handle_output(self, context, obj: Any):
        """Handles writing the pandas DataFrame to SQLite."""
        df = obj
        table_name = context.asset_key.path[-1]
        
        # Connect to SQLite database (it will create the database if it doesn't exist)
        conn = sqlite3.connect(self.database_uri)
        
        # Write the dataframe to the table (if it exists, it will overwrite)
        df.to_sql(table_name, conn, if_exists='replace', index=False)

        conn.close()

    def load_input(self, context) -> Any:
        """Handles reading from the SQLite database."""
        table_name = context.asset_key.path[-1]
        
        # Connect to SQLite database
        conn = sqlite3.connect(self.database_uri)
        
        # Read data from the specified table
        df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
        
        conn.close()
        
        return df

# Define the SQLite IO Manager as a resource for use in assets
@io_manager
def sqlite_io_manager(init_context):
    # Specify the database URI where data will be stored
    return SQLiteIOManager(database_uri="news_portal.db")
