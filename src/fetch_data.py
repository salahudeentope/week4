import os
# os.system("pip install -r requirements.txt")
import requests
import pandas as pd
import schedule
import time
from decouple import config


# api-query
url = config("URL")
querystring = config("QUERY_STRING")
headers = {
	"x-rapidapi-key": config("API_KEY"),
	"x-rapidapi-host": "free-football-api-data.p.rapidapi.com"
}

# Paths to files
parquet_path = "/Users/topesalahudeen/Desktop/TDI/week4/output/file.parquet.gzip"
excel_path = "/Users/topesalahudeen/Desktop/TDI/week4/outputfile.xlsx"

# Function to normalize JSON data and convert to DataFrame
def get_normalized_df(json_data):
    # Normalize the JSON data to flatten nested structures
    df = pd.json_normalize(json_data)
    
    # Remove 'response.event.' prefix from column headers
    df.columns = df.columns.str.replace('response.event.', '', regex=False)
    
    return df

# Function to append DataFrame to Parquet file
def append_to_parquet(df, path):
    if os.path.exists(path):
        # Append data to the existing Parquet file
        existing_df = pd.read_parquet(path, engine="fastparquet")
        df = pd.concat([existing_df, df], ignore_index=True)
    # Write the updated DataFrame to Parquet
    df.to_parquet(path, compression="gzip", engine="fastparquet")

# Function to append DataFrame to Excel file
def append_to_excel(df, path):
    if os.path.exists(path):
        # Append data to the existing Excel file
        existing_df = pd.read_excel(path)
        df = pd.concat([existing_df, df], ignore_index=True)
    # Write the updated DataFrame to Excel
    df.to_excel(path, index=False)

# Function to fetch data from the API, append it, and write to both Parquet and Excel
def fetch_and_append_data():
    response = requests.get(url, headers=headers, params=querystring)
    if response.status_code == 200:
        print("Data retrieved successfully!")
        data = response.json()
        print(data)
        
        # Convert the JSON data to a normalized DataFrame
        df = get_normalized_df(data)
        
        # Append the DataFrame to Parquet and Excel files
        append_to_parquet(df, parquet_path)
        append_to_excel(df, excel_path)
        
        print(f"Data appended to {parquet_path} and {excel_path}.")
    else:
        print("Failed to retrieve data. Status code:", response.status_code)
        
if __name__ == "__main__":
    fetch_and_append_data()