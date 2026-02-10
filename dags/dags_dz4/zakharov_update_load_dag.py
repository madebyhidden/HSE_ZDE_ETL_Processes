from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
from datetime import datetime, timedelta
from airflow.models.param import Param

default_args = {
    'owner': 'Denis Zakharov',
    'start_date': datetime(2026, 2, 3),
    'retries': 1
}

PG_CONN_ID = 'postgres_default'
FILE_PATH = '/opt/airflow/data/IOT-temp_new.csv'
TARGET_TABLE = 'iot_weather_results' 
DAYS_FOR_UPDATE = 2

def etl_update_load(**context):

    date_str = context['params']['force_run_date']
    
    try:
        execution_date = datetime.strptime(date_str, '%Y-%m-%d').date()
    except ValueError:
        print(f"Ошибка даты: {date_str}")
        return




    start_date = execution_date - timedelta(days=DAYS_FOR_UPDATE)


    df = pd.read_csv(FILE_PATH)
    df['noted_date'] = pd.to_datetime(df['noted_date'], dayfirst=True).dt.date
    df = df[(df['noted_date'] > start_date) & (df['noted_date'] <= execution_date)]

    if df.empty:
        print(f"за период с {start_date} по {execution_date} ничего нового нет")
        return


    df = df[df['out/in'] == 'In']
    q_05 = df['temp'].quantile(0.05)
    q_95 = df['temp'].quantile(0.95)
    df = df[(df['temp'] >= q_05) & (df['temp'] <= q_95)]

    daily_max_temps = df.groupby('noted_date')['temp'].max().reset_index()
    daily_min_temps = df.groupby('noted_date')['temp'].min().reset_index()
    
    result_df = pd.concat([daily_max_temps, daily_min_temps]).drop_duplicates(subset=['noted_date'])

    if result_df.empty:
        print("нет данных")
        return

    rows_to_insert = list(result_df[['noted_date', 'temp']].itertuples(index=False, name=None))
    pg_hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    
    for row in rows_to_insert:
        pg_hook.run(
            f"""
            INSERT INTO {TARGET_TABLE} (event_date, temperature)
            VALUES (%s, %s)
            ON CONFLICT (event_date)
            DO UPDATE SET temperature = EXCLUDED.temperature;
            """,
            parameters=row
        )


    pg_hook.run("""
        CREATE TABLE IF NOT EXISTS iot_update_log (
            load_time TIMESTAMP DEFAULT NOW(),
            event_date DATE,
            temperature FLOAT
        );
    """)
    pg_hook.insert_rows(
        table='iot_update_log',
        rows=rows_to_insert,
        target_fields=['event_date', 'temperature']
    )


with DAG(
    dag_id='dz_update_load',
    default_args=default_args,
    schedule_interval='@daily', 
    catchup=False,
    params={
        'force_run_date': Param(

            default="2026-02-01", 
            type='string',
            title="Force Run Date",
            description="Этот параметр нужен только для того, чтобы появилась форма."
        )
    },
    tags=['iot', 'update']
) as dag:

    update_load_task = PythonOperator(
        task_id='update_load_task',
        python_callable=etl_update_load,
    )