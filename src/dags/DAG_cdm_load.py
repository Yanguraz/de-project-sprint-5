# LOAD DATA TO CDM

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
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.models.xcom import XCom
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.task_group import TaskGroup

# Настройки PostgreSQL
pg_connection = PostgresHook.get_connection('PG_WAREHOUSE_CONNECTION')

# Инициализация подключения к базе данных PostgreSQL
conn_1 = psycopg2.connect(
    f"""
    host='{pg_connection.host}'
    port='{pg_connection.port}'
    dbname='{pg_connection.schema}' 
    user='{pg_connection.login}' 
    password='{pg_connection.password}'
    """
    ) 


# courier_ledger TABLE
def load_paste_data_dm_courier_ledger():
    fetching_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    current_table = 'courier_ledger'
    
    # courier_ledger
    cur_1 = conn_1.cursor()                 
    postgres_insert_query = """ 
        WITH t1_courier_calculation AS (
            SELECT 
                c.courier_id,
                c.courier_name,
                o.order_id,
                o.sum,
                t.year,
                t.month,
                AVG(d.rate)::float AS rate_avg,
                CASE
                    WHEN AVG(d.rate) < 4 THEN 
                        CASE
                            WHEN 0.05 * o.sum <= 100 THEN 100
                            ELSE 0.05 * o.sum
                        END
                    WHEN AVG(d.rate) >= 4 AND AVG(d.rate) < 4.5 THEN 
                        CASE
                            WHEN 0.07 * o.sum <= 150 THEN 150
                            ELSE 0.07 * o.sum
                        END
                    WHEN AVG(d.rate) >= 4.5 AND AVG(d.rate) < 4.9 THEN 
                        CASE
                            WHEN 0.08 * o.sum <= 175 THEN 175
                            ELSE 0.08 * o.sum
                        END
                    WHEN AVG(d.rate) >= 4.9 THEN 
                        CASE
                            WHEN 0.1 * o.sum <= 200 THEN 200
                            ELSE 0.1 * o.sum
                        END
                END AS courier_order_sum
            FROM 
                dds.deliveries d
            INNER JOIN 
                dds.couriers c ON d.courier_id = c.id
            INNER JOIN 
                dds.timestamps t ON d.delivery_ts = t.ts
            INNER JOIN 
                dds.orders o ON d.order_id = o.id
            GROUP BY 
                c.courier_id,
                c.courier_name,
                o.order_id,
                o.sum,
                t.year,
                t.month
        ),
        t2_agg_tips AS (
            SELECT 
                courier_id,
                courier_name,
                "year",
                "month",
                SUM(courier_order_sum) AS courier_order_sum
            FROM 
                t1_courier_calculation
            GROUP BY 
                courier_id,
                courier_name,
                "year",
                "month"
        )
        INSERT INTO cdm.dm_courier_ledger (courier_id, courier_name, settlement_year, settlement_month, orders_count, order_total_sum, rate_avg, order_processing_fee, courier_order_sum, courier_tips_sum, courier_reward_sum)
        SELECT 
            c.courier_id,
            c.courier_name,
            t.year,
            t.month,
            COUNT(d.delivery_id) AS orders_count,
            SUM(o."sum") AS order_total_sum,
            AVG(d.rate)::float AS rate_avg,
            SUM(o."sum") * 0.25 AS order_processing_fee,
            t3.courier_order_sum AS courier_order_sum,
            SUM(d.tip_sum) AS courier_tips_sum,
            (t3.courier_order_sum + 0.95 * SUM(d.tip_sum)) AS courier_reward_sum
        FROM 
            dds.deliveries d
        INNER JOIN 
            dds.couriers c ON d.courier_id = c.id
        INNER JOIN 
            dds.timestamps t ON d.delivery_ts = t.ts
        INNER JOIN 
            dds.orders o ON d.order_id = o.id
        INNER JOIN 
            t2_agg_tips AS t3 ON (c.courier_id = t3.courier_id AND t.year = t3.year AND t.month = t3.month)
        GROUP BY 
            c.courier_id,
            c.courier_name,
            t.year,
            t.month,
            courier_order_sum
        ON CONFLICT (courier_id, settlement_year, settlement_month) DO UPDATE
        SET 
            courier_name = EXCLUDED.courier_name,
            orders_count = EXCLUDED.orders_count,
            order_total_sum = EXCLUDED.order_total_sum,
            rate_avg = EXCLUDED.rate_avg,
            order_processing_fee = EXCLUDED.order_processing_fee,
            courier_order_sum = EXCLUDED.courier_order_sum,
            courier_tips_sum = EXCLUDED.courier_tips_sum,
            courier_reward_sum = EXCLUDED.courier_reward_sum;
    """
    
    # settings
    postgres_insert_query_settings = """ 
        INSERT INTO dds.settings (workflow_key, workflow_settings) 
        VALUES ('{}','{}');""".format(fetching_time,current_table)                  
    cur_1.execute(postgres_insert_query)    
    cur_1.execute(postgres_insert_query_settings)  
    
    conn_1.commit()
    conn_1.close()
    cur_1.close()

create_all_tables_cdm = PostgresOperator(
    task_id="create_cdm_tables",
    postgres_conn_id='PG_WAREHOUSE_CONNECTION',
    sql="./sql_scripts/create_cdm_tables.sql",
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
        'load_cdm_to_postgres',
        default_args=default_args,
        schedule_interval='0/15 * * * *',
        start_date=datetime(2024, 4, 22),
        catchup=False,
        tags=['sprint5', 'project'],
) as dag:

    task1 = DummyOperator(task_id="start")
    with TaskGroup("load_cdm_tables") as load_tables:
        task21 = PythonOperator(task_id="courier_ledger", python_callable=load_paste_data_dm_courier_ledger, dag=dag)
    task4 = DummyOperator(task_id="end")
    
    task1 >> create_all_tables_cdm >> load_tables >> task4


