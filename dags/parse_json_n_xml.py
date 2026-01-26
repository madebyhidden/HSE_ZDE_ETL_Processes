from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime

default_args = {
    'owner': 'Denis Zakharov',
    'start_date': datetime(2026, 1, 26),
    'retries': 0
}

with DAG(
    'parse_json_n_xml',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['homework', 'sql']
) as dag:

    parse_json_task = SQLExecuteQueryOperator(
        task_id='parse_json_pets',
        conn_id='postgres_default', 
        sql="""
            DROP TABLE IF EXISTS target_json_pets;

            CREATE TABLE target_json_pets AS
            SELECT
                item->>'name' AS pet_name,
                item->>'species' AS species,
                (item->>'birthYear')::int AS birth_year,
                item->>'favFoods' AS fav_foods, 
                item->>'photo' AS photo_url
            FROM 
                json_content,
                jsonb_array_elements(data->'pets') AS item;
        """
    )

    parse_xml_task = SQLExecuteQueryOperator(
        task_id='parse_xml_food',
        conn_id='postgres_default',
        sql="""
            DROP TABLE IF EXISTS target_xml_food;

            CREATE TABLE target_xml_food AS
            SELECT
                parsed_xml.food_name,
                parsed_xml.manufacturer,
                parsed_xml.calories,
                parsed_xml.total_fat,
                parsed_xml.protein
            FROM 
                xml_content,
                XMLTABLE(
                    '//food'            
                    PASSING data         
                    COLUMNS 
                        food_name    text PATH 'name',             
                        manufacturer text PATH 'mfr',              
                        calories     int  PATH 'calories/@total', 
                        total_fat    text PATH 'total-fat',
                        protein      text PATH 'protein'
                ) AS parsed_xml;
        """
    )

    [parse_json_task, parse_xml_task]