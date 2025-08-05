Stock OHLC Data Pipeline for Technical Analysis
This repository contains a robust Python-based data pipeline designed to ingest, clean, transform, validate, and store historical Open, High, Low, and Close (OHLC) stock data. This pipeline is crucial for a hedge fund's technical analysis team, providing a standardized and enriched dataset for further analysis and model development.

Project Overview
The primary goal of this project is to automate the processing of daily stock data feeds, ensuring data quality and preparing it for advanced financial modeling.

Features
Data Ingestion:

Fetches historical OHLC data for multiple stock tickers from Yahoo Finance (yfinance).

Handles potential MultiIndex columns from yfinance to ensure consistent column naming.

Performs initial data type validation and checks for missing values.

Data Cleaning:

Manages missing values using forward and backward fill techniques.

Implements basic outlier detection and capping using the Interquartile Range (IQR) method for 'close' price and 'volume'.

Ensures consistent date/timestamp formats.

Data Transformation:

Calculates key technical indicators: Simple Moving Averages (SMA), Exponential Moving Averages (EMA), Bollinger Bands (BB), and Relative Strength Index (RSI).

Engineers new features such as daily returns, annualized volatility, and close-open price differences.

Data Validation:

Includes a validate_pipeline function with assertion-based checks to ensure data integrity and expected structure after each processing stage.

Data Storage:

Stores processed data in a local SQLite database (ohlc_data.db).

Partitions data by year and month for efficient querying.

Provides a retrieve_data function for flexible data retrieval.

Technologies Used
Python 3.x

pandas: For data manipulation and analysis.

numpy: For numerical operations.

yfinance: For fetching historical stock data.

sqlite3: For local database storage.

Setup Instructions
Clone the Repository:

git clone https://github.com/your-username/stock-ohlc-pipeline.git # Replace with your actual repo URL
cd stock-ohlc-pipeline

Create and Activate Virtual Environment (Recommended):

python -m venv ohlc_pipeline_env
# On Windows:
.\ohlc_pipeline_env\Scripts\activate
# On macOS/Linux:
source ohlc_pipeline_env/bin/activate

Install Dependencies:

pip install pandas numpy yfinance

Usage
Place the script: Ensure the main_pipeline.py script (containing the pipeline code) is located inside your ohlc_pipeline_env directory.

Run the pipeline:

(ohlc_pipeline_env) PS D:\OHLC\ohlc_pipeline_env> python main_pipeline.py

(Note: The (ohlc_pipeline_env) PS D:\OHLC\ohlc_pipeline_env> part is your terminal prompt; you only type python main_pipeline.py)

The script will fetch data for the configured tickers, process it, and store it in ohlc_data.db in the same directory. Console output will show the progress and sample results.

Example Output from a Recent Run
--- Starting Data Pipeline ---
Removed existing database file: ohlc_data.db

--- Processing AAPL ---
Ingesting data for AAPL from 2020-01-01 to 2025-08-06...
Original columns for AAPL: [('Close', 'AAPL'), ('High', 'AAPL'), ('Low', 'AAPL'), ('Open', 'AAPL'), ('Volume', 'AAPL')]
Column structure type: <class 'pandas.core.indexes.multi.MultiIndex'>
Processed columns for AAPL: ['close', 'high', 'low', 'open', 'volume']
Successfully ingested 1405 rows for AAPL.
Cleaning data...
Data cleaning complete. Remaining rows: 1405
Transforming data: calculating technical indicators and features...
Data transformation complete.
Performing data validation checks...
All data validation checks passed successfully.
Storing data into ohlc_data.db in table ohlc_processed_data...
Successfully stored 1405 rows into table 'ohlc_processed_data'.
✓ Successfully processed AAPL

--- Processing MSFT ---
Ingesting data for MSFT from 2020-01-01 to 2025-08-06...

1 Failed download:
['MSFT']: ConnectionError('Failed to perform, curl: (56) Recv failure: Connection was reset. See https://curl.se/libcurl/c/libcurl-errors.html first for more details.')
No data found for MSFT in the specified range.
Skipping MSFT due to ingestion failure.

--- Processing GOOG ---
Ingesting data for GOOG from 2020-01-01 to 2025-08-06...
Original columns for GOOG: [('Close', 'GOOG'), ('High', 'GOOG'), ('Low', 'GOOG'), ('Open', 'GOOG'), ('Volume', 'GOOG')]
Column structure type: <class 'pandas.core.indexes.multi.MultiIndex'>
Processed columns for GOOG: ['close', 'high', 'low', 'open', 'volume']
Successfully ingested 1405 rows for GOOG.
Cleaning data...
Data cleaning complete. Remaining rows: 1405
Transforming data: calculating technical indicators and features...
Data transformation complete.
Performing data validation checks...
All data validation checks passed successfully.
Storing data into ohlc_data.db in table ohlc_processed_data...
Successfully stored 1405 rows into table 'ohlc_processed_data'.
✓ Successfully processed GOOG

--- Processing AMZN ---
Ingesting data for AMZN from 2020-01-01 to 2025-08-06...
Original columns for AMZN: [('Close', 'AMZN'), ('High', 'AMZN'), ('Low', 'AMZN'), ('Open', 'AMZN'), ('Volume', 'AMZN')]
Column structure type: <class 'pandas.core.indexes.multi.MultiIndex'>
Processed columns for AMZN: ['close', 'high', 'low', 'open', 'volume']
Successfully ingested 1405 rows for AMZN.
Cleaning data...
Data cleaning complete. Remaining rows: 1405
Transforming data: calculating technical indicators and features...
Data transformation complete.
Performing data validation checks...
All data validation checks passed successfully.
Storing data into ohlc_data.db in table ohlc_processed_data...
Successfully stored 1405 rows into table 'ohlc_processed_data'.
✓ Successfully processed AMZN

--- Processing TSLA ---
Ingesting data for TSLA from 2020-01-01 to 2025-08-06...
Original columns for TSLA: [('Close', 'TSLA'), ('High', 'TSLA'), ('Low', 'TSLA'), ('Open', 'TSLA'), ('Volume', 'TSLA')]
Column structure type: <class 'pandas.core.indexes.multi.MultiIndex'>
Processed columns for TSLA: ['close', 'high', 'low', 'open', 'volume']
Successfully ingested 1405 rows for TSLA.
Cleaning data...
Data cleaning complete. Remaining rows: 1405
Transforming data: calculating technical indicators and features...
Data transformation complete.
Performing data validation checks...
All data validation checks passed successfully.
Storing data into ohlc_data.db in table ohlc_processed_data...
Successfully stored 1405 rows into table 'ohlc_processed_data'.
✓ Successfully processed TSLA

--- Processing NVDA ---
Ingesting data for NVDA from 2020-01-01 to 2025-08-06...
Original columns for NVDA: [('Close', 'NVDA'), ('High', 'NVDA'), ('Low', 'NVDA'), ('Open', 'NVDA'), ('Volume', 'NVDA')]
Column structure type: <class 'pandas.core.indexes.multi.MultiIndex'>
Processed columns for NVDA: ['close', 'high', 'low', 'open', 'volume']
Successfully ingested 1405 rows for NVDA.
Cleaning data...
Data cleaning complete. Remaining rows: 1405
Transforming data: calculating technical indicators and features...
Data transformation complete.
Performing data validation checks...
All data validation checks passed successfully.
Storing data into ohlc_data.db in table ohlc_processed_data...
Successfully stored 1405 rows into table 'ohlc_processed_data'.
✓ Successfully processed NVDA

--- Processing JPM ---
Ingesting data for JPM from 2020-01-01 to 2025-08-06...
Original columns for JPM: [('Close', 'JPM'), ('High', 'JPM'), ('Low', 'JPM'), ('Open', 'JPM'), ('Volume', 'JPM')]
Column structure type: <class 'pandas.core.indexes.multi.MultiIndex'>
Processed columns for JPM: ['close', 'high', 'low', 'open', 'volume']
Successfully ingested 1405 rows for JPM.
Cleaning data...
Data cleaning complete. Remaining rows: 1405
Transforming data: calculating technical indicators and features...
Data transformation complete.
Performing data validation checks...
All data validation checks passed successfully.
Storing data into ohlc_data.db in table ohlc_processed_data...
Successfully stored 1405 rows into table 'ohlc_processed_data'.
✓ Successfully processed JPM

--- Processing V ---
Ingesting data for V from 2020-01-01 to 2025-08-06...
Original columns for V: [('Close', 'V'), ('High', 'V'), ('Low', 'V'), ('Open', 'V'), ('Volume', 'V')]
Column structure type: <class 'pandas.core.indexes.multi.MultiIndex'>
Processed columns for V: ['close', 'high', 'low', 'open', 'volume']
Successfully ingested 1405 rows for V.
Cleaning data...
Data cleaning complete. Remaining rows: 1405
Transforming data: calculating technical indicators and features...
Data transformation complete.
Performing data validation checks...
All data validation checks passed successfully.
Storing data into ohlc_data.db in table ohlc_processed_data...
Successfully stored 1405 rows into table 'ohlc_processed_data'.
✓ Successfully processed V

--- Processing PG ---
Ingesting data for PG from 2020-01-01 to 2025-08-06...
Original columns for PG: [('Close', 'PG'), ('High', 'PG'), ('Low', 'PG'), ('Open', 'PG'), ('Volume', 'PG')]
Column structure type: <class 'pandas.core.indexes.multi.MultiIndex'>
Processed columns for PG: ['close', 'high', 'low', 'open', 'volume']
Successfully ingested 1405 rows for PG.
Cleaning data...
Data cleaning complete. Remaining rows: 1405
Transforming data: calculating technical indicators and features...
Data transformation complete.
Performing data validation checks...
All data validation checks passed successfully.
Storing data into ohlc_data.db in table ohlc_processed_data...
Successfully stored 1405 rows into table 'ohlc_processed_data'.
✓ Successfully processed PG

--- Processing KO ---
Ingesting data for KO from 2020-01-01 to 2025-08-06...
Original columns for KO: [('Close', 'KO'), ('High', 'KO'), ('Low', 'KO'), ('Open', 'KO'), ('Volume', 'KO')]
Column structure type: <class 'pandas.core.indexes.multi.MultiIndex'>
Processed columns for KO: ['close', 'high', 'low', 'open', 'volume']
Successfully ingested 1405 rows for KO.
Cleaning data...
Data cleaning complete. Remaining rows: 1405
Transforming data: calculating technical indicators and features...
Data transformation complete.
Performing data validation checks...
All data validation checks passed successfully.
Storing data into ohlc_data.db in table ohlc_processed_data...
Successfully stored 1405 rows into table 'ohlc_processed_data'.
✓ Successfully processed KO

--- Data Pipeline Complete ---
Successfully processed 9 out of 10 tickers:
Success: ['AAPL', 'GOOG', 'AMZN', 'TSLA', 'NVDA', 'JPM', 'V', 'PG', 'KO']
Failed: ['MSFT']

--- Sample Results ---
Retrieving data from ohlc_data.db table ohlc_processed_data...
Retrieved 1405 rows.

First 5 rows for AAPL:
             open       high        low      close       volume ticker    SMA_10  ...   BB_Lower         RSI  Daily_Return  Volatility_20D  Close_Open_Diff  year  month
date                                                                                                                                                                       

2020-01-02  71.627084  72.681281  71.373211  72.620834  135480400.0   AAPL  72.620834  ...        NaN        NaN           NaN             NaN         0.993750  2020      1
2020-01-03  71.847133  72.676462  71.689973  71.914833  146322800.0   AAPL  72.267834  ...  71.269397   0.000000   -0.009722             NaN         0.067700  2020      1
2020-01-06  71.034724  72.526549  70.783263  72.487862  118387200.0   AAPL  72.341176  ...  71.590851  44.801813    0.007968        0.198569         1.453137  2020      1
2020-01-07  72.497514  72.753808  71.926900  72.146927  108872000.0   AAPL  72.292614  ...  71.649918  35.372907   -0.004703        0.144723        -0.350587  2020      1
2020-01-08  71.849533  73.609745  71.849533  73.307510  132079200.0   AAPL  72.495593  ...  71.430790  62.347854    0.016086        0.186869         1.457977  2020      1

[5 rows x 20 columns]

Last 5 rows for AAPL:
             open       high        low      close      volume ticker  ...        RSI  Daily_Return  Volatility_20D  Close_Open_Diff  year  month
date                                                                   ...                                                                           
2025-07-30  211.899994  212.389999  207.720001  209.050003  45512500.0   AAPL  ...  39.353620   -0.010508        0.141704        -2.849991  2025      7
2025-07-31  208.490005  209.839996  207.160004  207.570007  80698400.0   AAPL  ...  38.788276   -0.007080        0.117809        -0.919998  2025      7
2025-08-01  210.869995  213.580002  201.500000  202.380005 104434500.0   AAPL  ...  33.279772   -0.025004        0.142459        -8.489990  2025      8
2025-08-04  204.509995  207.880005  201.679993  203.350006  75109300.0   AAPL  ...  34.952992    0.004793        0.134305        -1.159988  2025      8
2025-08-05  203.434998  205.339996  202.160004  202.919998  41840671.0   AAPL  ...  30.453553   -0.002115        0.134137        -0.514999  2025      8

[5 rows x 20 columns]

DataFrame info for AAPL:
<class 'pandas.core.frame.DataFrame'>
DatetimeIndex: 1405 entries, 2020-01-02 to 2025-08-05
Data columns (total 20 columns):
 #   Column           Non-Null Count  Dtype  
---  ------           --------------  -----  
 0   open             1405 non-null   float64
 1   high             1405 non-null   float64
 2   low              1405 non-null   float64
 3   close            1405 non-null   float64
 4   volume           1405 non-null   float64
 5   ticker           1405 non-null   object 
 6   SMA_10           1405 non-null   float64
 7   SMA_50           1405 non-null   float64
 8   EMA_10           1405 non-null   float64
 9   EMA_50           1405 non-null   float64
 10  BB_Middle        1405 non-null   float64
 11  BB_StdDev        1404 non-null   float64
 12  BB_Upper         1404 non-null   float64
 13  BB_Lower         1404 non-null   float64
 14  RSI              1404 non-null   float64
 15  Daily_Return     1404 non-null   float64
 16  Volatility_20D   1403 non-null   float64
 17  Close_Open_Diff  1405 non-null   float64
 18  year             1405 non-null   int64  
 19  month            1405 non-null   int64  
dtypes: float64(17), int64(2), object(1)
memory usage: 230.5+ KB
None
