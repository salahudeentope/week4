import os
import requests
import pandas as pd
from decouple import config
from load_to_postgres import save_to_postgresql  # Import the save_to_postgresql function
from transform_data import transform_data  # Import the transform_data function

# API query details
url = "https://jsearch.p.rapidapi.com/estimated-salary"
querystring = {"job_title": "NodeJS Developer", "location": "New-York, NY, USA", "radius": "100"}
headers = {
    "x-rapidapi-key": config('API_KEY'),
    "x-rapidapi-host": "jsearch.p.rapidapi.com"
}

# Paths to files
parquet_path = "/Users/topesalahudeen/Desktop/TDI/week4/output/file.parquet.gzip"
excel_path = "/Users/topesalahudeen/Desktop/TDI/week4/output/file.xlsx"

# Function to normalize JSON data and convert to DataFrame
def get_normalized_df(json_data):
    df = pd.json_normalize(json_data)
    return df

# Function to append DataFrame to Parquet file
def append_to_parquet(df, path):
    if os.path.exists(path):
        existing_df = pd.read_parquet(path, engine="fastparquet")
        df = pd.concat([existing_df, df], ignore_index=True)
    df.to_parquet(path, compression="gzip", engine="fastparquet")

# Function to append DataFrame to Excel file
def append_to_excel(df, path):
    if os.path.exists(path):
        existing_df = pd.read_excel(path)
        df = pd.concat([existing_df, df], ignore_index=True)
    df.to_excel(path, index=False)

# Function to fetch data from the API, append it, and write to both Parquet, Excel, and PostgreSQL
def fetch_data():
    response = requests.get(url, headers=headers, params=querystring)
    if response.status_code == 200:
        print("Data retrieved successfully!")
        data = response.json()['data']
        # df = get_normalized_df(data)
        
        # # Transform salary data (Hourly and Monthly to Yearly)
        # df = transform_data(df)
        
        # # # # Append the DataFrame to Parquet and Excel files
        # # append_to_parquet(df, parquet_path)
        # # append_to_excel(df, excel_path)
        
        # # # Save the DataFrame to PostgreSQL
        # # save_to_postgresql(df, 'salary_data')
        # # # print(df)
        # print(f"Data appended to {parquet_path}, {excel_path}, and PostgreSQL.")
        return data
    else:
        print("Failed to retrieve data. Status code:", response.status_code)

if __name__ == "__main__":
    fetch_and_append_data()
