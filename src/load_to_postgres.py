from sqlalchemy import create_engine
import pandas as pd
from decouple import config

# PostgreSQL connection configuration
DB_USER = config('DB_USER')
DB_PASSWORD = config('DB_PASSWORD')
DB_HOST = config('DB_HOST')
DB_PORT = config('DB_PORT')
DB_NAME = config('DB_NAME')

# Create a PostgreSQL engine
engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

# Function to save DataFrame to PostgreSQL using SQLAlchemy
def save_to_postgresql(df: pd.DataFrame, table_name: str):
    try:
        # Append to the existing PostgreSQL table
        df.to_sql(table_name, con=engine, if_exists='append', index=False)
        print(f"Data appended to PostgreSQL table '{table_name}' successfully.")
    except Exception as e:
        print(f"Failed to save data to PostgreSQL: {e}")
