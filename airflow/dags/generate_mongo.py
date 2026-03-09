from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from datetime import datetime, timedelta
import random
import uuid

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 3, 9),
    'retries': 1,
}

def generate_streaming_data():
    hook = MongoHook(mongo_conn_id='mongo_default')
    client = hook.get_conn()
    db = client.streaming_db 
    

    db.ViewingSessions.drop()
    db.AppFeedbacks.drop()


    sessions = []
    platforms = ['SmartTV', 'iOS', 'Android', 'Web']
    content_ids = ['movie_1', 'movie_2', 'series_5_ep1', 'series_5_ep2']
    
    for _ in range(150):
        start_time = datetime.now() - timedelta(days=random.randint(0, 30), minutes=random.randint(10, 600))
        end_time = start_time + timedelta(minutes=random.randint(5, 180))
        sessions.append({
            "session_id": f"view_{uuid.uuid4().hex[:8]}",
            "user_id": f"user_{random.randint(1, 50)}",
            "content_id": random.choice(content_ids),
            "start_time": start_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "end_time": end_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "device_info": {"platform": random.choice(platforms), "app_version": "2.1.0"},
            "events": ["play", "pause", "resume"]
        })
    db.ViewingSessions.insert_many(sessions)

 
    feedbacks = []
    types = ['bug_report', 'review', 'feature_request']
    statuses = ['open', 'resolved']
    
    for _ in range(80):
        created_at = datetime.now() - timedelta(days=random.randint(2, 30))
        status = random.choice(statuses)

        resolved_at = created_at + timedelta(hours=random.randint(1, 48)) if status == 'resolved' else None
        
        feedbacks.append({
            "feedback_id": f"fb_{uuid.uuid4().hex[:8]}",
            "user_id": f"user_{random.randint(1, 50)}",
            "type": random.choice(types),
            "status": status,
            "created_at": created_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "resolved_at": resolved_at.strftime("%Y-%m-%dT%H:%M:%SZ") if resolved_at else None,
            "rating": random.randint(1, 5) if type == 'review' else None
        })
    db.AppFeedbacks.insert_many(feedbacks)

with DAG('1_generate_mongo_data', default_args=default_args, schedule_interval=None, catchup=False) as dag:
    gen_data_task = PythonOperator(
        task_id='generate_mock_data',
        python_callable=generate_streaming_data
    )