import time
import logging
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timezone, timedelta
import pandas as pd
import csv
import os 

def ensure_data_directory(exch, suffix=None):
    """Ensures the data directory exists."""
    path = f"{exch}_data" if suffix is None else f"{exch}_data/{suffix}"
    if not os.path.exists(path):
        os.makedirs(path)

def get_csv_filename(exch, suffix, tz= timezone.utc):
        """Generates a CSV filename based on the current date (YYYY-MM-DD)."""
        # Get the current date and format it as YYYY-MM-DD
        today_str = datetime.fromtimestamp(time.time(), tz=tz).strftime("%Y-%m-%d")
        # Construct the full path for the CSV file
        return os.path.join(f"{exch}_data/{suffix}", f"{exch}_{today_str}.csv")

def get_parquet_filename(exch, suffix, tz= timezone.utc):
    today_str = datetime.fromtimestamp(time.time(), tz=tz).strftime("%Y-%m-%d")
    #return os.path.join(f"{exch}_data", f"{exch}_{suffix}_{today_str}.parquet")
    return os.path.join(f"{exch}_data/{suffix}", f"{exch}_{suffix}_{today_str}.parquet")

def write_to_csv(exch, data, headers, suffix, logger):
        """
        Writes a dictionary of data to a CSV file.
        It expects 'data' to be a dictionary where keys are column headers.
        You might need to adjust this function based on the actual structure
        of the data returned by the Loris API.
        """
        filename = get_csv_filename(exch, suffix)
        file_exists = os.path.exists(filename)
    
        if not data: return
    
        try:
            with open(filename, 'a', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=headers)
            
                if not file_exists:
                    writer.writeheader()  # Write header only if the file is new
                    logger.info(f"Created new CSV file: {filename} with headers: {headers}")
            
                for data_raw in data: writer.writerow(data_raw)
                logger.info(f"Data successfully written to {filename}.")
        except IOError as e:
            logger.error(f"Error writing to CSV file {filename}: {e}")
        except Exception as e:
            logger.error(f"An unexpected error occurred while writing data to CSV: {e}")

def append_to_parquetfile(file_path, exch, data, schema, partition_cols, dataFormaterFct, logger):
    #sucess = False
    try:
        if data is None: return
        if isinstance(data, pd.DataFrame) and data.empty: return # Check for pandas DataFrame
        if hasattr(data, '__len__') and len(data) < 1: return

        df = dataFormaterFct(data)
        pq.write_to_dataset(
            df,
            root_path=file_path,
            partition_cols=partition_cols
            )
        logger.info(f"[{exch}]: Batch written successfully.")
        #sucess = True
    except Exception as e: logger.error(f"write_to_parquetfile: an unexpected error occurred while writing data to parquet file: {e}")
    #finally: return sucess