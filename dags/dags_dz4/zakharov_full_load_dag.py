from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
from datetime import datetime


default_args = {
    'owner': 'Denis Zakharov',
    'start_date': datetime(2026, 2, 10),
    'retries': 0
}

PG_CONN_ID = 'postgres_default'
FILE_PATH = '/opt/airflow/data/IOT-temp.csv'
TARGET_TABLE = 'iot_weather_results'

def etl_full_load():
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
    
    create_table_sql = f"""
        DROP TABLE IF EXISTS {TARGET_TABLE};
        CREATE TABLE {TARGET_TABLE} (
            event_date DATE PRIMARY KEY,
            temperature FLOAT
        );

        DROP TABLE IF EXISTS iot_update_log;
        CREATE TABLE iot_update_log (
            load_time TIMESTAMP DEFAULT NOW(),
            event_date DATE,
            temperature FLOAT
        );
    """
    pg_hook.run(create_table_sql)
    
    pg_hook.insert_rows(
        table=TARGET_TABLE,
        rows=rows_to_insert,
        target_fields=['event_date', 'temperature']
    )
    
    pg_hook.insert_rows(
        table='iot_update_log',
        rows=rows_to_insert,
        target_fields=['event_date', 'temperature'] 
    )


with DAG(
    dag_id='dz_full_load',  
    default_args=default_args,
    schedule_interval=None, 
    catchup=False,
    tags=['iot', 'full_load']
) as dag:

    full_load_task = PythonOperator(
        task_id='full_load_task',
        python_callable=etl_full_load
    )