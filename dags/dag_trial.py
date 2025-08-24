from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import requests
import pendulum

def print_welcome():
    print('Welcome to Airflow!')

def print_date():
    print(f"Today is {datetime.today().date()}")

def print_random_quote():
    try:
        r = requests.get("https://zenquotes.io/api/random", timeout=10)
        r.raise_for_status()
        data = r.json()
        quote = data[0].get('q', 'Keep it simple.')
        author = data[0].get('a', 'Unknown')
        print(f'Quote of the day: "{quote}" — {author}')
    except Exception as e:
        print(f'Quote of the day: "Done is better than perfect." — Unknown (fallback due to: {e})')

# Define a timezone-aware start_date
local_tz = pendulum.timezone("America/Chicago")

dag = DAG(
    'welcome_dag',
    default_args={'start_date': pendulum.datetime(2025, 8, 24, 0, 0, tz=local_tz)},
    schedule='0 10 * * *',   # 10:00 Chicago time
    catchup=False
)

print_welcome_task = PythonOperator(
    task_id='print_welcome',
    python_callable=print_welcome,
    dag=dag
)

print_date_task = PythonOperator(
    task_id='print_date',
    python_callable=print_date,
    dag=dag
)

print_random_quote_task = PythonOperator(
    task_id='print_random_quote',
    python_callable=print_random_quote,
    dag=dag
)

print_welcome_task >> print_date_task >> print_random_quote_task
