import os
import pandas as pd
from prefect import task, flow
from fetch_data import fetch_data
from transform_data import transform_data
from load_to_postgres import save_to_postgresql
from datetime import timedelta, datetime
from prefect.client.schemas.schedules import IntervalSchedule


# Paths to files
parquet_path = "/Users/topesalahudeen/Desktop/TDI/week4/output/file.parquet.gzip"
excel_path = "/Users/topesalahudeen/Desktop/TDI/week4/output/file.xlsx"

# Prefect Task for Fetching and Appending the Json Data
@task
def get_data():
    # Your data fetching logic
    json_data = fetch_data()

    return json_data  # Return the fetched data

# Prefect Task for normalizing JSON data into a DataFrame
@task
def normalize_data(json_data):
    if isinstance(json_data, list):
        # If it's a list of dictionaries
        df = pd.json_normalize(json_data)
    elif isinstance(json_data, dict):
        # If it's a single dictionary
        df = pd.json_normalize(json_data)
    else:
        raise ValueError("Input data must be a list or a dictionary.")
    return df

# Prefect Task to append DataFrame to Parquet file
@task
def append_to_parquet(df):
    if os.path.exists(parquet_path):
        existing_df = pd.read_parquet(parquet_path, engine="fastparquet")
        df = pd.concat([existing_df, df], ignore_index=True)
    df.to_parquet(parquet_path, compression="gzip", engine="fastparquet")
    print(f"Data saved to {parquet_path}")

# Prefect Task to append DataFrame to Excel file
@task
def append_to_excel(df):
    if os.path.exists(excel_path):
        existing_df = pd.read_excel(excel_path)
        df = pd.concat([existing_df, df], ignore_index=True)
    df.to_excel(excel_path, index=False)
    print(f"Data saved to {excel_path}")

# Prefect Task for loading data into PostgreSQL
@task
def load_to_postgresql(df):
    save_to_postgresql(df, 'salary_data')

# Define the Prefect Flow for the ETL process
@flow
def etl_pipeline():
    # Fetch data
    json_data = get_data()
    # print(json_data)
    if json_data is None:
        raise ValueError("No data fetched. json_data is None.")
    
    # print(f"Fetched JSON Data: {json_data}")  # Debug statement

    # Normalize and transform data
    df_normalized = normalize_data(json_data)
    df_transformed = transform_data(df_normalized)
    
    # Append to Parquet and Excel
    append_to_parquet(df_transformed)
    append_to_excel(df_transformed)
    
    # Load into PostgreSQL
    load_to_postgresql(df_transformed)

# To run the flow, use this command or schedule it
if __name__ == "__main__":
     etl_pipeline.serve(schedules=[
    IntervalSchedule(
      interval=timedelta(seconds=60),
        anchor_date=datetime(2024, 9, 28),
        timezone="America/Chicago"
        )
    ]
    )

