from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 3, 9),
    'retries': 1,
}


sql_dm_content = """
CREATE TABLE IF NOT EXISTS dm_content_popularity AS
SELECT 
    DATE(start_time) as watch_date,
    platform,
    content_id,
    COUNT(DISTINCT session_id) as total_views,
    COUNT(DISTINCT user_id) as unique_viewers,
    AVG(watch_duration_minutes) as avg_watch_time_min
FROM viewing_sessions
GROUP BY DATE(start_time), platform, content_id;

-- Перезапись витрины актуальными данными
TRUNCATE TABLE dm_content_popularity;
INSERT INTO dm_content_popularity
SELECT 
    DATE(start_time) as watch_date,
    platform,
    content_id,
    COUNT(DISTINCT session_id) as total_views,
    COUNT(DISTINCT user_id) as unique_viewers,
    AVG(watch_duration_minutes) as avg_watch_time_min
FROM viewing_sessions
GROUP BY DATE(start_time), platform, content_id;
"""

sql_dm_quality = """
CREATE TABLE IF NOT EXISTS dm_app_quality AS
SELECT 
    DATE(created_at) as report_date,
    feedback_type,
    status,
    COUNT(feedback_id) as total_reports,
    AVG(resolution_time_hours) as avg_resolution_hours
FROM app_feedbacks
GROUP BY DATE(created_at), feedback_type, status;

TRUNCATE TABLE dm_app_quality;
INSERT INTO dm_app_quality
SELECT 
    DATE(created_at) as report_date,
    feedback_type,
    status,
    COUNT(feedback_id) as total_reports,
    AVG(resolution_time_hours) as avg_resolution_hours
FROM app_feedbacks
GROUP BY DATE(created_at), feedback_type, status;
"""

with DAG('3_build_datamarts', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:
    build_dm_content = PostgresOperator(
        task_id='build_dm_content_popularity',
        postgres_conn_id='postgres_default',
        sql=sql_dm_content
    )

    build_dm_quality = PostgresOperator(
        task_id='build_dm_app_quality_metrics',
        postgres_conn_id='postgres_default',
        sql=sql_dm_quality
    )

    [build_dm_content, build_dm_quality]