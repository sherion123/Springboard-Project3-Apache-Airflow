from airflow import DAG
from pathlib import Path 
from datetime import datetime, timedelta
import os
import pandas as pd
import yfinance as yf
#import requests
import pendulum

from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
#---------------------------------------------------------------------------------------------------------------------------
#                                                                CONFIG
#---------------------------------------------------------------------------------------------------------------------------
# NEEDED FOR FUNCTIONS AND OPERATORS BELOW 
SYMBOLS = ["AAPL", "TSLA"]

# Where this DAG file lives (inside the container it's /opt/airflow/dags) ---
BASE_DIR = Path(__file__).resolve().parent

# DIRECTORIES
TMP_DIR = "/tmp/data/{{ ds }}"                  # temporary download folder
OUTPUT_DIR = str(BASE_DIR / "output" / "{{ ds }}")     # final “data” folder for both CSVs

#---------------------------------------------------------------------------------------------------------------------------
#                                                                TASK CALLABLES
#---------------------------------------------------------------------------------------------------------------------------
# Function for t1 and t2:
# 2.2. Create a PythonOperator to download the market data (t1, t2)
#      This example downloads and saves the file. Make your own function the Airflow runs with stock symbol as parameter
#      Python Operator should call the function below.
def download_to_tmp(symbol: str, ds: str, **_):
    """
    Download 1-minute bars for the execution date (ds) and save CSV under /tmp/data/{{ ds }}/<SYMBOL>.csv
    Matches the mini-project’s simple approach (parameterized by symbol).
    """
    start_date = datetime.strptime(ds, "%Y-%m-%d").date()
    end_date = start_date + timedelta(days=1)

    df = yf.download(symbol, start=start_date, end=end_date, interval="1m", progress=False)

    out_dir = f"/tmp/data/{ds}"
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, f"{symbol}.csv")

    df.to_csv(out_path, index=True, header=False)
    print(f"[download_to_tmp] wrote {out_path}")
    return out_path

# Function for t5: 
# 2.4. Create a PythonOperator to run a query on both data files in the specified location
#      (t5)
#      For this step, run your query on the data you downloaded for both symbols. This step should run
#      only when t3 and t4 are completed.
def simple_query(ds: str, **_):
    """
    Read both CSVs from OUTPUT_DIR and print tiny analytics:
    - row count
    - total 'volume' (assumes volume is the last column if header=False)
    """
    base = OUTPUT_DIR.replace("{{ ds }}", ds)
    results = []
    for sym in SYMBOLS:
        path = os.path.join(base, f"{sym}.csv")
        if not os.path.exists(path):
            print(f"[simple_query] WARNING: missing {path}")
            continue

        
        df = pd.read_csv(path, header=None)
        # Assume last column is Volume (typical yfinance CSV order)
        vol = df.iloc[:, -1].fillna(0).sum() if df.shape[1] >= 1 else 0
        results.append((sym, len(df), float(vol)))

    print("Symbol,Rows,TotalVolume")
    for sym, rows, vol in results:
        print(f"{sym},{rows},{vol}")
    return results

#---------------------------------------------------------------------------------------------------------------------------
#                                                                DAG
#---------------------------------------------------------------------------------------------------------------------------

# Default Arguments:
# NOTES: Some of the default arguments will be specified in default_args(specifically retries and retry interval)
#        Other arguments will be in the DAG creation.
#  Start time and date: 6PM on the current date
#  Job interval: Runs once daily
#  Only runs on weekdays (Mon-Fri)
#  If failed: Retry twice with a 5 minute interval
default_args = {"owner": 'airflow'
                ,"retries": 2
                ,"retry_delay": timedelta(minutes=5)}

# Specify timezone
tz = pendulum.timezone("America/Chicago")
start_at = pendulum.today(tz).at(18, 0, 0)  # 6 PM today (tz-aware)

# Creating DAG:
dag = DAG(
    'marketvol',
    default_args=default_args,
    start_date=start_at,
    schedule='0 18 * * 1-5',   # 6 PM Mon-Fri
    catchup=False
)

#---------------------------------------------------------------------------------------------------------------------------
#                                                               OPERATORS AND TASKS
#---------------------------------------------------------------------------------------------------------------------------
# 2.1. Create a BashOperator to initialize a temporary directory for data download (t0)
#      This temporary directory should be named after the execution date (for example “2020-09-24”)
#      so that the data for each date is placed accordingly. You can use the shell command to create
#      the directory:

t0 = BashOperator(
    task_id='t0',
    bash_command=f"mkdir -p {TMP_DIR}",
    dag=dag
)

# 2.2. Create a PythonOperator to download the market data (t1, t2)
#      This example downloads and saves the file. Make your own function the Airflow runs with the
#      stock symbol type as a parameter.
#      The PythonOperator should call the above function. Name the operators t1 and t2 for the
#      symbol AAPL and TSLA respectively.
t1 = PythonOperator(
    task_id='t1_aapl',
    python_callable=download_to_tmp,
    op_kwargs={"symbol": "AAPL",
               "ds": "{{ ds }}"},
    dag=dag
)

t2 = PythonOperator(
    task_id='t2_tsla',
    python_callable=download_to_tmp,
    op_kwargs={"symbol": "TSLA",
               "ds": "{{ ds }}"},
    dag=dag
)

# 2.3. Create BashOperator to move the downloaded file to a data location (t3, t4)
t3 = BashOperator(
        task_id="t3_move_aapl",
        params={"symbol": "AAPL"},
        bash_command=(
            f"mkdir -p {OUTPUT_DIR} && "
            f"mv {TMP_DIR}/{{{{ params.symbol }}}}.csv {OUTPUT_DIR}/{{{{ params.symbol }}}}.csv"
        ),
        dag=dag
    )

t4 = BashOperator(
        task_id="t4_move_tsla",
        params={"symbol": "TSLA"},
        bash_command=(
            f"mkdir -p {OUTPUT_DIR} && "
            f"mv {TMP_DIR}/{{{{ params.symbol }}}}.csv {OUTPUT_DIR}/{{{{ params.symbol }}}}.csv"
        ),
        dag=dag
    )

# 2.4. Create a PythonOperator to run a query on both data files in the specified location
#      (t5)
#      This step should run only when t3 and t4 are completed.

t5 = PythonOperator(
    task_id="t5_query_both",
    python_callable=simple_query,
    op_kwargs={"ds": "{{ ds }}"},
    dag=dag
)

#---------------------------------------------------------------------------------------------------------------------------
#                                                              SET JOB DEPENDENCIES
#---------------------------------------------------------------------------------------------------------------------------
# After defining all the tasks, you need to set their job dependencies so:
# - t1 and t2 must run only after t0
# - t3 must run after t1
# - t4 must run after t2
# - t5 must run after both t3 and t4 are complete

t0 >> [t1, t2]
t1 >> t3
t2 >> t4
[t3, t4] >> t5