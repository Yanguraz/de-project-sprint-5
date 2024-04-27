# ЗАГРУЗКА ДАННЫХ В STG 

from datetime import datetime, timedelta
import time
import pandas as pd
import numpy as np
import psycopg2
import json
from wsgiref import headers
import requests

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.task_group import TaskGroup

### Настройки API ###
# Устанавливаем параметры для запросов API
sort_field = "id"
sort_direction = "asc"
limit = 50
offset = 0
nickname = "batuqagan"
cohort = "22"
api_key = "25c27781-8fde-4b30-a22e-524044a7580f"
url_restaurants = "https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/restaurants" 
url_couriers = "https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/couriers" 
url_deliveries = "https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/deliveries" 


class InputParams:
    """Параметры по умолчанию для API"""
    headers = {
        "X-Nickname" : nickname,
        "X-Cohort" : cohort,
        "X-API-KEY" : api_key
        }
    params = {
        "sort_field" : sort_field, 
        "sort_direction" : sort_direction,
        "limit" : limit,
        "offset" : offset
        }

### Настройки PostgreSQL ###
pg_connection = PostgresHook.get_connection('PG_WAREHOUSE_CONNECTION')

# Инициализируем соединение
# Подключаемся к Postgres
pg_connection = PostgresHook('PG_WAREHOUSE_CONNECTION')
conn_1 = pg_connection.get_conn()

def get_last_offset(table_name):
    cur = conn_1.cursor()
    cur.execute(f"SELECT offset_param FROM stg.last_offset WHERE table_name = '{table_name}'")
    row = cur.fetchone()
    cur.close()
    return row[0] if row else 0

def update_last_offset(table_name, offset_value):
    cur = conn_1.cursor()
    cur.execute(f"INSERT INTO stg.last_offset (table_name, offset_param) VALUES ('{table_name}', {offset_value}) ON CONFLICT (table_name) DO UPDATE SET offset_param = EXCLUDED.offset_param")
    conn_1.commit()
    cur.close()

# restaurants
def load_insert_data_restaurants():
    offset = get_last_offset('restaurants')
    InputParams.params['offset'] = offset

    r = requests.get(
        url = url_restaurants, 
        params = InputParams.params, 
        headers = InputParams.headers)
    json_record = json.loads(r.content)
    temp_list_restaurants = []
    while len(json_record) != 0 :
        r = requests.get(url = url_restaurants, params = InputParams.params, headers = InputParams.headers)
        json_record = json.loads(r.content)
        for obj in json_record:
            temp_list_restaurants.append(obj)
        InputParams.params['offset'] += 50
    if temp_list_restaurants:
        temp_list_restaurants = json.dumps(temp_list_restaurants)

        fetching_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        current_table = 'restaurants'
        
        # Загрузка в restaurants
        cur_1 = conn_1.cursor()                 
        postgres_insert_query = """ 
            INSERT INTO stg.restaurants (object_value,update_ts) 
            VALUES ('{}', '{}');""".format(temp_list_restaurants,fetching_time)
        
        # Загрузка в settings
        postgres_insert_query_settings = """ 
            INSERT INTO stg.settings (workflow_key, workflow_settings) 
            VALUES ('{}','{}');""".format(fetching_time,current_table)                  
        cur_1.execute(postgres_insert_query)    
        cur_1.execute(postgres_insert_query_settings)  
        
        # Апдейтим оффсет
        update_last_offset('restaurants', InputParams.params['offset'])
        
        conn_1.commit()
        cur_1.close()

# couriers
def load_insert_data_couriers():
    offset = get_last_offset('couriers')
    InputParams.params['offset'] = offset
    
    r = requests.get(
        url = url_couriers, 
        params = InputParams.params, 
        headers = InputParams.headers)
    json_record = json.loads(r.content)
    temp_list_couriers = []
    while len(json_record) != 0 :
        r = requests.get(url = url_couriers, params = InputParams.params, headers = InputParams.headers)
        json_record = json.loads(r.content)
        for obj in json_record:
            temp_list_couriers.append(obj)
        InputParams.params['offset'] += 50
    if temp_list_couriers:
        temp_list_couriers = json.dumps(temp_list_couriers)

        fetching_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        current_table = 'couriers'
        
        # Загрузка в couriers
        cur_1 = conn_1.cursor()                 
        postgres_insert_query = """ 
            INSERT INTO stg.couriers (object_value,update_ts) 
            VALUES ('{}', '{}');""".format(temp_list_couriers,fetching_time)
        
        # Загрузка в settings
        postgres_insert_query_settings = """ 
            INSERT INTO stg.settings (workflow_key, workflow_settings) 
            VALUES ('{}','{}');""".format(fetching_time,current_table)                  
        cur_1.execute(postgres_insert_query)    
        cur_1.execute(postgres_insert_query_settings)  
        
        # Апдейтим оффсет
        update_last_offset('couriers', InputParams.params['offset'])
        
        conn_1.commit()
        cur_1.close()

# deliveries
def load_insert_data_deliveries():
    offset = get_last_offset('deliveries')
    InputParams.params['offset'] = offset
    
    r = requests.get(
        url = url_deliveries, 
        params = InputParams.params, 
        headers = InputParams.headers)
    json_record = json.loads(r.content)
    temp_list_deliveries = []
    while len(json_record) != 0 :
        r = requests.get(url = url_deliveries, params = InputParams.params, headers = InputParams.headers)
        json_record = json.loads(r.content)
        for obj in json_record:
            temp_list_deliveries.append(obj)
        InputParams.params['offset'] += 50
    if temp_list_deliveries:
        temp_list_deliveries = json.dumps(temp_list_deliveries)

        fetching_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        current_table = 'deliveries'
        
        # Загрузка deliveries
        cur_1 = conn_1.cursor()                 
        postgres_insert_query = """ 
            INSERT INTO stg.deliveries (object_value,update_ts) 
            VALUES ('{}', '{}');""".format(temp_list_deliveries,fetching_time)
        
        # Загрузка в settings
        postgres_insert_query_settings = """ 
            INSERT INTO stg.settings (workflow_key, workflow_settings) 
            VALUES ('{}','{}');""".format(fetching_time,current_table)                  
        cur_1.execute(postgres_insert_query)    
        cur_1.execute(postgres_insert_query_settings)  
        
        # Апдейтим оффсет
        update_last_offset('deliveries', InputParams.params['offset'])
        
        conn_1.commit()
        cur_1.close()

creates_stg_tables = PostgresOperator(
    task_id="creates_stg_tables",
    postgres_conn_id='PG_WAREHOUSE_CONNECTION',
    sql="/sql_scripts/create_stg_tables.sql",
)

default_args = {
    'owner': 'Airflow',
    'schedule_interval':'@once',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'catchup':False
}

with DAG(
        'load_stg_to_postgres',
        default_args=default_args,
        schedule_interval='0/15 * * * *',
        start_date=datetime(2024, 4, 22),
        catchup=False,
        tags=['sprint5', 'project'],
) as dag:

    # DAG exec
    task1 = DummyOperator(task_id="start")
    with TaskGroup("loading_stg_tables") as load_tables:
        task2_1 = PythonOperator(task_id="restaurants", python_callable=load_insert_data_restaurants, dag=dag)
        task2_2 = PythonOperator(task_id="couriers", python_callable=load_insert_data_couriers, dag=dag)
        task2_3 = PythonOperator(task_id="deliveries", python_callable=load_insert_data_deliveries, dag=dag)
    task4 = DummyOperator(task_id="end")
    
    task1 >> creates_stg_tables >> load_tables >> task4




