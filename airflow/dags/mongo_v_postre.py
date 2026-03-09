from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import dateutil.parser

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 3, 9),
    'retries': 1,
}

def etl_viewing_sessions():
    mongo_hook = MongoHook(mongo_conn_id='mongo_default')
    sessions = list(mongo_hook.get_collection('ViewingSessions', 'streaming_db').find())
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    records = []
    for s in sessions:
        
        start = dateutil.parser.parse(s['start_time'])
        end = dateutil.parser.parse(s['end_time'])
        duration = int((end - start).total_seconds() / 60)
        
      
        platform = s.get('device_info', {}).get('platform', 'unknown')
        
        records.append((
            s['session_id'], s['user_id'], s['content_id'], start, end, platform, duration
        ))


    insert_query = """
        INSERT INTO viewing_sessions (session_id, user_id, content_id, start_time, end_time, platform, watch_duration_minutes)
        VALUES %s
        ON CONFLICT (session_id) DO UPDATE SET
            end_time = EXCLUDED.end_time,
            watch_duration_minutes = EXCLUDED.watch_duration_minutes;
    """
    pg_hook.insert_rows(table="viewing_sessions", rows=records, replace=False, target_fields_query=insert_query)

def etl_feedbacks():
    mongo_hook = MongoHook(mongo_conn_id='mongo_default')
    feedbacks = list(mongo_hook.get_collection('AppFeedbacks', 'streaming_db').find())
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    records = []
    for f in feedbacks:
        created = dateutil.parser.parse(f['created_at'])
        resolved = dateutil.parser.parse(f['resolved_at']) if f.get('resolved_at') else None
        

        res_hours = None
        if resolved:
            res_hours = round((resolved - created).total_seconds() / 3600, 2)
            
        records.append((
            f['feedback_id'], f['user_id'], f['type'], f['status'], created, resolved, res_hours
        ))

    insert_query = """
        INSERT INTO app_feedbacks (feedback_id, user_id, feedback_type, status, created_at, resolved_at, resolution_time_hours)
        VALUES %s
        ON CONFLICT (feedback_id) DO UPDATE SET
            status = EXCLUDED.status,
            resolved_at = EXCLUDED.resolved_at,
            resolution_time_hours = EXCLUDED.resolution_time_hours;
    """
    pg_hook.insert_rows(table="app_feedbacks", rows=records, replace=False, target_fields_query=insert_query)

with DAG('2_mongo_to_postgres_etl', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:
    task_sessions = PythonOperator(task_id='etl_viewing_sessions', python_callable=etl_viewing_sessions)
    task_feedbacks = PythonOperator(task_id='etl_app_feedbacks', python_callable=etl_feedbacks)

    [task_sessions, task_feedbacks]