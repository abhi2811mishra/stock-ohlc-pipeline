import pandas as pd
import numpy as np
import yfinance as yf
import sqlite3
from datetime import datetime, timedelta
import os
import time


TICKERS = ['AAPL', 'MSFT', 'GOOG', 'AMZN', 'TSLA', 'NVDA', 'JPM', 'V', 'PG', 'KO']

START_DATE = '2020-01-01'
END_DATE = datetime.now().strftime('%Y-%m-%d') 

DB_NAME = 'ohlc_data.db'


def ingest_data(ticker: str, start: str, end: str, retry_count: int = 3) -> pd.DataFrame:
    """
    Ingests OHLC data for a given ticker from Yahoo Finance.
    Performs initial data validation and standardizes the format.
    Includes retry logic for network issues.

    Args:
        ticker (str): The stock ticker symbol (e.g., 'AAPL').
        start (str): Start date in 'YYYY-MM-DD' format.
        end (str): End date in 'YYYY-MM-DD' format.
        retry_count (int): Number of retry attempts for failed downloads.

    Returns:
        pd.DataFrame: A DataFrame containing OHLC data with a standardized format.
                      Returns an empty DataFrame if data fetching fails or is empty.
    """
    print(f"Ingesting data for {ticker} from {start} to {end}...")
    
    for attempt in range(retry_count):
        try:
           
            if attempt > 0:
                print(f"Retry attempt {attempt + 1} for {ticker}...")
                time.sleep(2)
            
           
            df = yf.download(ticker, start=start, end=end, auto_adjust=True, progress=False)

            if df.empty:
                print(f"No data found for {ticker} in the specified range.")
                return pd.DataFrame()

        
            print(f"Original columns for {ticker}: {df.columns.tolist()}")
            print(f"Column structure type: {type(df.columns)}")
            
           
            if isinstance(df.columns, pd.MultiIndex):
               
                df.columns = df.columns.get_level_values(0)
            
           
            df.columns = [col.lower().replace(' ', '_') for col in df.columns]
            
            print(f"Processed columns for {ticker}: {df.columns.tolist()}")

            required_ohlcv_cols = ['open', 'high', 'low', 'close', 'volume']
            
           
            missing_cols = [col for col in required_ohlcv_cols if col not in df.columns]
            if missing_cols:
                print(f"Error: Missing required columns for {ticker}: {missing_cols}")
                print(f"Available columns: {df.columns.tolist()}")
                return pd.DataFrame()

            
            df = df[required_ohlcv_cols]

            
            df['ticker'] = ticker
            
            
            df.index.name = 'date'
            df.index = pd.to_datetime(df.index)

            
            numeric_cols = ['open', 'high', 'low', 'close', 'volume']
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')

            
            initial_missing = df.isnull().sum().sum()
            if initial_missing > 0:
                print(f"Warning: {initial_missing} missing values detected during ingestion for {ticker}.")

            print(f"Successfully ingested {len(df)} rows for {ticker}.")
            return df

        except Exception as e:
            print(f"Attempt {attempt + 1} failed for {ticker}: {e}")
            if attempt == retry_count - 1:
                print(f"All retry attempts failed for {ticker}")
                return pd.DataFrame()
    
    return pd.DataFrame()


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans the OHLC data by handling missing values and basic outlier detection.

    Args:
        df (pd.DataFrame): The raw OHLC DataFrame.

    Returns:
        pd.DataFrame: The cleaned DataFrame.
    """
    if df.empty:
        return df

    print("Cleaning data...")

    ohlcv_cols = ['open', 'high', 'low', 'close', 'volume']
    for col in ohlcv_cols:
        if col in df.columns:
            df[col] = df[col].ffill().bfill()


    df.dropna(subset=[col for col in ohlcv_cols if col in df.columns], how='all', inplace=True)

    for col in ['close', 'volume']:
        if col in df.columns and len(df) > 0:
            Q1 = df[col].quantile(0.25)
            Q3 = df[col].quantile(0.75)
            IQR = Q3 - Q1
            
            if IQR > 0: 
                lower_bound = Q1 - 1.5 * IQR
                upper_bound = Q3 + 1.5 * IQR

                df[col] = np.where(df[col] < lower_bound, lower_bound, df[col])
                df[col] = np.where(df[col] > upper_bound, upper_bound, df[col])

    df.sort_index(inplace=True)

    print(f"Data cleaning complete. Remaining rows: {len(df)}")
    return df

def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms the OHLC data by calculating technical indicators and new features.

    Args:
        df (pd.DataFrame): The cleaned OHLC DataFrame.

    Returns:
        pd.DataFrame: The transformed DataFrame with new features.
    """
    if df.empty:
        return df

    print("Transforming data: calculating technical indicators and features...")

    if 'close' not in df.columns:
        print("Error: 'close' column not found for technical indicator calculation.")
        return df

    if len(df) >= 10:
        df['SMA_10'] = df['close'].rolling(window=10, min_periods=1).mean()
    if len(df) >= 50:
        df['SMA_50'] = df['close'].rolling(window=50, min_periods=1).mean()

    if len(df) >= 10:
        df['EMA_10'] = df['close'].ewm(span=10, adjust=False).mean()
    if len(df) >= 50:
        df['EMA_50'] = df['close'].ewm(span=50, adjust=False).mean()

  
    if len(df) >= 20:
        window = 20
        df['BB_Middle'] = df['close'].rolling(window=window, min_periods=1).mean()
        df['BB_StdDev'] = df['close'].rolling(window=window, min_periods=1).std()
        df['BB_Upper'] = df['BB_Middle'] + (df['BB_StdDev'] * 2)
        df['BB_Lower'] = df['BB_Middle'] - (df['BB_StdDev'] * 2)

    
    if len(df) >= 14:
        delta = df['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14, min_periods=1).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14, min_periods=1).mean()
        
    
        RS = gain / loss.where(loss != 0, np.nan)
        df['RSI'] = 100 - (100 / (1 + RS))

   
    df['Daily_Return'] = df['close'].pct_change()
    if len(df) >= 20:
        df['Volatility_20D'] = df['Daily_Return'].rolling(window=20, min_periods=1).std() * np.sqrt(252)

   
    if 'open' in df.columns:
        df['Close_Open_Diff'] = df['close'] - df['open']

    print("Data transformation complete.")
    return df


def validate_pipeline(df: pd.DataFrame) -> bool:
    """
    Performs data validation checks on the processed DataFrame.

    Args:
        df (pd.DataFrame): The processed DataFrame.

    Returns:
        bool: True if all validations pass, False otherwise.
    """
    print("Performing data validation checks...")
    try:
    
        assert not df.empty, "Validation Error: DataFrame is empty after processing."

      
        essential_cols = ['open', 'high', 'low', 'close', 'volume', 'ticker']
        for col in essential_cols:
            assert col in df.columns, f"Validation Error: Missing essential column '{col}'."

    
        numeric_cols = ['open', 'high', 'low', 'close', 'volume']
        for col in numeric_cols:
            if col in df.columns:
                assert pd.api.types.is_numeric_dtype(df[col]), f"Validation Error: '{col}' is not numeric."

        
        assert isinstance(df.index, pd.DatetimeIndex), "Validation Error: DataFrame index is not DatetimeIndex."

       
        assert (df['close'] > 0).all(), "Validation Error: Negative or zero close prices found."

        
        for col in ['close', 'volume']:
            if col in df.columns:
                nan_percentage = df[col].isnull().sum() / len(df)
                assert nan_percentage < 0.1, f"Validation Error: Too many NaN values in '{col}' ({nan_percentage:.2%})"

        print("All data validation checks passed successfully.")
        return True

    except AssertionError as e:
        print(f"Data Validation Failed: {e}")
        return False
    except Exception as e:
        print(f"An unexpected error occurred during validation: {e}")
        return False


def store_data(df: pd.DataFrame, db_name: str, table_name: str):
    """
    Stores the processed DataFrame into a SQLite database.

    Args:
        df (pd.DataFrame): The DataFrame to store.
        db_name (str): The name of the SQLite database file.
        table_name (str): The name of the table to store data in.
    """
    if df.empty:
        print(f"No data to store for table {table_name}.")
        return

    print(f"Storing data into {db_name} in table {table_name}...")
    conn = None
    try:
        conn = sqlite3.connect(db_name)

        
        df['year'] = df.index.year
        df['month'] = df.index.month

        
        df.to_sql(table_name, conn, if_exists='append', index=True, index_label='date')
        print(f"Successfully stored {len(df)} rows into table '{table_name}'.")

    except sqlite3.Error as e:
        print(f"SQLite error during data storage: {e}")
    except Exception as e:
        print(f"An unexpected error occurred during data storage: {e}")
    finally:
        if conn:
            conn.close()

def retrieve_data(db_name: str, table_name: str, ticker: str = None, year: int = None, month: int = None) -> pd.DataFrame:
    """
    Retrieves data from the SQLite database with optional filtering.

    Args:
        db_name (str): The name of the SQLite database file.
        table_name (str): The name of the table to retrieve data from.
        ticker (str, optional): Filter by stock ticker.
        year (int, optional): Filter by year.
        month (int, optional): Filter by month.

    Returns:
        pd.DataFrame: The retrieved data.
    """
    print(f"Retrieving data from {db_name} table {table_name}...")
    conn = None
    try:
        conn = sqlite3.connect(db_name)
        query = f"SELECT * FROM {table_name}"
        conditions = []
        if ticker:
            conditions.append(f"ticker = '{ticker}'")
        if year:
            conditions.append(f"year = {year}")
        if month:
            conditions.append(f"month = {month}")

        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        query += " ORDER BY date;"

        df = pd.read_sql_query(query, conn, index_col='date', parse_dates=['date'])
        print(f"Retrieved {len(df)} rows.")
        return df
    except sqlite3.Error as e:
        print(f"SQLite error during data retrieval: {e}")
        return pd.DataFrame()
    except Exception as e:
        print(f"An unexpected error occurred during data retrieval: {e}")
        return pd.DataFrame()
    finally:
        if conn:
            conn.close()

def run_data_pipeline():
    """
    Orchestrates the entire data pipeline process for multiple tickers.
    """
    print("--- Starting Data Pipeline ---")
    successful_tickers = []

    
    if os.path.exists(DB_NAME):
        os.remove(DB_NAME)
        print(f"Removed existing database file: {DB_NAME}")

    for ticker in TICKERS:
        print(f"\n--- Processing {ticker} ---")
        try:
           
            raw_df = ingest_data(ticker, START_DATE, END_DATE)
            if raw_df.empty:
                print(f"Skipping {ticker} due to ingestion failure.")
                continue

            
            cleaned_df = clean_data(raw_df.copy())
            if cleaned_df.empty:
                print(f"Skipping {ticker} due to cleaning failure.")
                continue

            
            transformed_df = transform_data(cleaned_df.copy())
            if transformed_df.empty:
                print(f"Skipping {ticker} due to transformation failure.")
                continue

           
            if not validate_pipeline(transformed_df):
                print(f"Skipping {ticker} due to validation failure.")
                continue

          
            store_data(transformed_df, DB_NAME, 'ohlc_processed_data')
            successful_tickers.append(ticker)
            print(f"✓ Successfully processed {ticker}")

        except Exception as e:
            print(f"✗ Failed to process {ticker}: {e}")
            continue

    print("\n--- Data Pipeline Complete ---")
    print(f"Successfully processed {len(successful_tickers)} out of {len(TICKERS)} tickers:")
    print(f"Success: {successful_tickers}")
    
    failed_tickers = [t for t in TICKERS if t not in successful_tickers]
    if failed_tickers:
        print(f"Failed: {failed_tickers}")

    
    if successful_tickers and os.path.exists(DB_NAME):
        print("\n--- Sample Results ---")
        sample_ticker = successful_tickers[0]
        sample_data = retrieve_data(DB_NAME, 'ohlc_processed_data', ticker=sample_ticker)
        if not sample_data.empty:
            print(f"\nFirst 5 rows for {sample_ticker}:")
            print(sample_data.head())
            print(f"\nLast 5 rows for {sample_ticker}:")
            print(sample_data.tail())
            print(f"\nDataFrame info for {sample_ticker}:")
            print(sample_data.info())
    else:
        print("No data was successfully processed and stored.")


if __name__ == "__main__":
    run_data_pipeline()