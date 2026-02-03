from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
from datetime import datetime
import os




default_args = {
    'owner': 'Denis Zakharov',
    'start_date': datetime(2026, 2, 3),
    'retries': 0
}

PG_CONN_ID = 'postgres_default'
FILE_PATH = '/opt/airflow/data/IOT-temp.csv'

def etl_process():

    print(f' ЭТО ЛОГ ДИРЕКТОРИИ {os.listdir()}')

    df = pd.read_csv(FILE_PATH)
    df = df[df['out/in'] == 'In']

    df['noted_date'] = pd.to_datetime(df['noted_date'], dayfirst=True).dt.date

    q_05 = df['temp'].quantile(0.05)
    q_95 = df['temp'].quantile(0.95)
    df = df[(df['temp'] >= q_05) & (df['temp'] <= q_95)]


    daily_max_temps = df.groupby('noted_date')['temp'].max().reset_index()
    daily_min_temps = df.groupby('noted_date')['temp'].min().reset_index()

    hottest_days = daily_max_temps.nlargest(5, 'temp')
    coldest_days = daily_min_temps.nsmallest(5, 'temp')
    
    result_df = pd.concat([hottest_days, coldest_days])

    rows_to_insert = list(result_df[['noted_date', 'temp']].itertuples(index=False, name=None))
    pg_hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    create_table_sql = """
        DROP TABLE IF EXISTS iot_weather_results;
        CREATE TABLE IF NOT EXISTS iot_weather_results (
            event_date DATE,
            temperature FLOAT
        );
    """
    pg_hook.run(create_table_sql)
    pg_hook.insert_rows(
        table='iot_weather_results',
        rows=rows_to_insert,
        target_fields=['event_date', 'temperature']
    )
    
    print(f"Успешно записано {len(rows_to_insert)} строк.")


with DAG(
    dag_id='dz_3_dataset',
     default_args=default_args,
    schedule_interval=None, 
    catchup=False
) as dag:

    process_task = PythonOperator(
        task_id='dz_3_dataset',
        python_callable=etl_process
    )

    process_task