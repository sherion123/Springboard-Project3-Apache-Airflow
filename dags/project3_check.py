from airflow import DAG
from pathlib import Path   
from datetime import datetime, timedelta
import os
import pandas as pd
import yfinance as yf
import pendulum

from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.sensors.time_delta import TimeDeltaSensor  # <-- NEW

# ---------------------------------------------------------------------------------------------------------------------------
#                                                                CONFIG
# ---------------------------------------------------------------------------------------------------------------------------
SYMBOLS = ["AAPL", "TSLA"]


# Where this DAG file lives (inside the container it's /opt/airflow/dags) ---
BASE_DIR = Path(__file__).resolve().parent

TMP_DIR = "/tmp/data/{{ ds }}"                  # temporary download folder
OUTPUT_DIR = str(BASE_DIR / "output" / "{{ ds }}")    # final “data” folder for both CSVs

# ---------------------------------------------------------------------------------------------------------------------------
#                                                                TASK CALLABLES
# ---------------------------------------------------------------------------------------------------------------------------
def download_to_tmp(symbol: str, ds: str, **_):
    start_date = datetime.strptime(ds, "%Y-%m-%d").date()
    end_date = start_date + timedelta(days=1)
    df = yf.download(symbol, start=start_date, end=end_date, interval="1m", progress=False)

    out_dir = f"/tmp/data/{ds}"
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, f"{symbol}.csv")
    df.to_csv(out_path, index=True, header=False)
    print(f"[download_to_tmp] wrote {out_path}")
    return out_path

def simple_query(ds: str, **_):
    base = OUTPUT_DIR.replace("{{ ds }}", ds)
    results = []
    for sym in SYMBOLS:
        path = os.path.join(base, f"{sym}.csv")
        if not os.path.exists(path):
            print(f"[simple_query] WARNING: missing {path}")
            continue
        try:
            df = pd.read_csv(path, header=None)
        except pd.errors.EmptyDataError:
            print(f"[simple_query] EMPTY FILE: {path} — skipping")
            continue
        vol = df.iloc[:, -1].fillna(0).sum() if df.shape[1] >= 1 else 0
        results.append((sym, len(df), float(vol)))

    print("Symbol,Rows,TotalVolume")
    for sym, rows, vol in results:
        print(f"{sym},{rows},{vol}")
    return results

# ---------------------------------------------------------------------------------------------------------------------------
#                                                                DAG
# ---------------------------------------------------------------------------------------------------------------------------
default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

tz = pendulum.timezone("America/Chicago")
start_at = pendulum.datetime(2024, 1, 1, 0, 0, 0, tz=tz)  

dag = DAG(
    "marketvol_quicktest",            
    default_args=default_args,
    start_date=start_at,
    schedule=None,                    # <-- run only when you Trigger DAG
    catchup=False,
    tags=["mini-project", "quicktest"],
)

# ---------------------------------------------------------------------------------------------------------------------------
#                                                               OPERATORS AND TASKS
# ---------------------------------------------------------------------------------------------------------------------------

# NEW: wait ~1 minute after you trigger, then proceed
t_wait = TimeDeltaSensor(
    task_id="t_wait_10min",
    delta=timedelta(minutes=1),      
    poke_interval=30,
    mode="reschedule",                # frees up worker slots while waiting
    dag=dag,
)

t0 = BashOperator(
    task_id="t0",
    bash_command=f"mkdir -p {TMP_DIR}",
    dag=dag,
)

t1 = PythonOperator(
    task_id="t1_aapl",
    python_callable=download_to_tmp,
    op_kwargs={"symbol": "AAPL", "ds": "{{ ds }}"},
    dag=dag,
)
t2 = PythonOperator(
    task_id="t2_tsla",
    python_callable=download_to_tmp,
    op_kwargs={"symbol": "TSLA", "ds": "{{ ds }}"},
    dag=dag,
)

t3 = BashOperator(
    task_id="t3_move_aapl",
    params={"symbol": "AAPL"},
    bash_command=(
        f"mkdir -p {OUTPUT_DIR} && "
        f"mv {TMP_DIR}/{{{{ params.symbol }}}}.csv {OUTPUT_DIR}/{{{{ params.symbol }}}}.csv"
    ),
    dag=dag,
)
t4 = BashOperator(
    task_id="t4_move_tsla",
    params={"symbol": "TSLA"},
    bash_command=(
        f"mkdir -p {OUTPUT_DIR} && "
        f"mv {TMP_DIR}/{{{{ params.symbol }}}}.csv {OUTPUT_DIR}/{{{{ params.symbol }}}}.csv"
    ),
    dag=dag,
)

t5 = PythonOperator(
    task_id="t5_query_both",
    python_callable=simple_query,
    op_kwargs={"ds": "{{ ds }}"},
    dag=dag,
)

# ---------------------------------------------------------------------------------------------------------------------------
#                                                              SET JOB DEPENDENCIES
# ---------------------------------------------------------------------------------------------------------------------------
t_wait >> t0
t0 >> [t1, t2]
t1 >> t3
t2 >> t4
[t3, t4] >> t5
