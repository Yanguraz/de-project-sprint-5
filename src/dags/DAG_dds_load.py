# Вставляем данные из stg в dds

from datetime import datetime, timedelta
import psycopg2
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
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

# restaurants
def load_paste_data_restaurants():
    fetching_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    current_table = 'restaurants'
    
    cur_1 = conn_1.cursor()                 
    postgres_insert_query = """ 
        WITH latest_settings AS (
            SELECT MAX(workflow_key::timestamp) AS max_timestamp
            FROM stg.settings
            WHERE workflow_settings = 'restaurants'
        ),
        latest_restaurants AS (
            SELECT DISTINCT 
                json_array_elements(r.object_value::JSON)->>'_id' AS restaurant_id,
                json_array_elements(r.object_value::JSON)->>'name' AS restaurant_name
            FROM stg.restaurants r
            INNER JOIN latest_settings s ON r.update_ts = s.max_timestamp
        )
        INSERT INTO dds.restaurants (restaurant_id, restaurant_name)
        SELECT lr.restaurant_id, lr.restaurant_name
        FROM latest_restaurants lr
        ON CONFLICT (restaurant_id) DO UPDATE 
        SET 
            restaurant_name = EXCLUDED.restaurant_name;
    """
    
    postgres_insert_query_settings = """ 
        INSERT INTO dds.settings (workflow_key, workflow_settings) 
        VALUES ('{}','{}');
        """.format(fetching_time,current_table)                  
    cur_1.execute(postgres_insert_query)    
    cur_1.execute(postgres_insert_query_settings)  
    
    conn_1.commit()
    conn_1.close()
    cur_1.close()

# couriers
def load_paste_data_couriers():
    fetching_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    current_table = 'couriers'
    
    cur_1 = conn_1.cursor()                 
    postgres_insert_query = """ 
        WITH latest_settings AS (
            SELECT MAX(workflow_key::timestamp) AS max_timestamp
            FROM stg.settings
            WHERE workflow_settings = 'couriers'
        ),
        latest_couriers AS (
            SELECT DISTINCT 
                json_array_elements(c.object_value::JSON)->>'_id' AS courier_id,
                json_array_elements(c.object_value::JSON)->>'name' AS courier_name
            FROM stg.couriers c
            INNER JOIN latest_settings s ON c.update_ts = s.max_timestamp
        )
        INSERT INTO dds.couriers (courier_id, courier_name)
        SELECT lc.courier_id, lc.courier_name
        FROM latest_couriers lc
        ON CONFLICT (courier_id) DO UPDATE 
        SET 
            courier_name = EXCLUDED.courier_name;
    """
    
    postgres_insert_query_settings = """ 
        INSERT INTO dds.settings (workflow_key, workflow_settings) 
        VALUES ('{}','{}');
        """.format(fetching_time,current_table)                  
    cur_1.execute(postgres_insert_query)    
    cur_1.execute(postgres_insert_query_settings)  
    
    conn_1.commit()
    conn_1.close()
    cur_1.close()

# timestamps
def load_paste_data_timestamps():
    fetching_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    current_table = 'timestamps'
    
    cur_1 = conn_1.cursor()                 
    postgres_insert_query = """ 
        WITH latest_settings AS (
            SELECT MAX(workflow_key::timestamp) AS max_timestamp
            FROM stg.settings
            WHERE workflow_settings = 'deliveries'
        ),
        t1_deliveries_ts AS (
            SELECT DISTINCT
                (json_array_elements(d.object_value::JSON) ->> 'delivery_ts')::timestamp AS delivery_ts
            FROM stg.deliveries d
            INNER JOIN latest_settings s ON d.update_ts = s.max_timestamp
        ),
        t2_orders_ts AS (
            SELECT DISTINCT
                (json_array_elements(d.object_value::JSON) ->> 'order_ts')::timestamp AS order_ts
            FROM stg.deliveries d
            INNER JOIN latest_settings s ON d.update_ts = s.max_timestamp
        ),
        t3_union AS (
            SELECT 
                delivery_ts AS ts
            FROM
                t1_deliveries_ts
            UNION ALL
            SELECT 
                order_ts AS ts
            FROM
                t2_orders_ts
        ),
        final_data AS (
            SELECT DISTINCT
                ts,
                EXTRACT(YEAR FROM ts) AS "year",
                EXTRACT(MONTH FROM ts) AS "month",
                EXTRACT(DAY FROM ts) AS "day",
                ts::time AS "time",
                ts::date AS "date"
            FROM
                t3_union
        )
        INSERT INTO dds.timestamps (ts, year, month, day, time, date)
        SELECT
            fd.ts,
            fd.year,
            fd.month,
            fd.day,
            fd.time,
            fd.date
        FROM
            final_data fd
        ON CONFLICT (ts) DO UPDATE 
        SET 
            "year" = EXCLUDED."year",
            "month" = EXCLUDED."month",
            "day" = EXCLUDED."day",
            "time" = EXCLUDED."time",
            "date" = EXCLUDED."date";

    """
    postgres_insert_query_settings = """ 
        INSERT INTO dds.settings (workflow_key, workflow_settings) 
        VALUES ('{}','{}');""".format(fetching_time,current_table)                      
    cur_1.execute(postgres_insert_query)    
    cur_1.execute(postgres_insert_query_settings)  
    
    conn_1.commit()
    conn_1.close()
    cur_1.close()

# orders
def load_paste_data_orders():
    fetching_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    current_table = 'orders'
    
    cur_1 = conn_1.cursor()                 
    postgres_insert_query = """ 
    insert into dds.orders (order_id, order_ts, "sum")
    select distinct 
        json_array_elements(d.object_value::JSON) ->> 'order_id'              as order_id,
        (json_array_elements(d.object_value::JSON) ->> 'order_ts')::timestamp as order_ts,
        (json_array_elements(d.object_value::JSON) ->> 'sum')::numeric(14,2)  as sum
    FROM stg.deliveries d
    INNER JOIN stg.settings s ON d.update_ts = s.workflow_key::timestamp
    WHERE s.workflow_key::timestamp =
        (SELECT MAX (workflow_key::timestamp)
        FROM stg.settings
        WHERE workflow_settings = 'deliveries' )
    on conflict (order_id) do update 
        set 
            order_id = excluded.order_id,
            order_ts = excluded.order_ts,
            "sum" = excluded."sum";
    """
    
    postgres_insert_query_settings = """ 
        INSERT INTO dds.settings (workflow_key, workflow_settings) 
        VALUES ('{}','{}');""".format(fetching_time,current_table)                
    cur_1.execute(postgres_insert_query)    
    cur_1.execute(postgres_insert_query_settings)  
    
    conn_1.commit()
    conn_1.close()
    cur_1.close()

# deliveries
def load_paste_data_deliveries():
    fetching_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    current_table = 'deliveries'
    
    cur_1 = conn_1.cursor()                 
    postgres_insert_query = """ 
    with t1_deliveries_part as (
    select
        json_array_elements(d.object_value::JSON) ->> 'delivery_id'               as delivery_id,
        json_array_elements(d.object_value::JSON) ->> 'order_id'                  as order_id,
        json_array_elements(d.object_value::JSON) ->> 'courier_id'                as courier_id,
        json_array_elements(d.object_value::JSON) ->> 'address'                   as address,
        (json_array_elements(d.object_value::JSON) ->> 'delivery_ts')::timestamp  as delivery_ts,
        (json_array_elements(d.object_value::JSON) ->> 'rate')::int               as rate,
        (json_array_elements(d.object_value::JSON) ->> 'tip_sum')::numeric(14,2)  as tip_sum
    FROM stg.deliveries d
    INNER JOIN stg.settings s ON d.update_ts = s.workflow_key::timestamp
    WHERE s.workflow_key::timestamp =
        (SELECT MAX (workflow_key::timestamp)
        FROM stg.settings
        WHERE workflow_settings = 'deliveries' )
    )     
    insert into dds.deliveries (delivery_id, courier_id, order_id, address, delivery_ts, rate, tip_sum)
    select distinct
        t1.delivery_id, 
        c.id as courier_id_one, 
        o.id as order_id_one, 
        t1.address, 
        t1.delivery_ts, 
        t1.rate, 
        t1.tip_sum 
    from
        t1_deliveries_part t1
            inner join dds.orders o on t1.order_id = o.order_id 
            inner join dds.couriers c on t1.courier_id = c.courier_id
    on conflict (delivery_id) do update 
        set 
            delivery_id = excluded.delivery_id,
            courier_id = excluded.courier_id,
            order_id = excluded.order_id,
            address = excluded.address,
            delivery_ts = excluded.delivery_ts,
            rate = excluded.rate,
            tip_sum = excluded.tip_sum;
    """
    
    postgres_insert_query_settings =  """ 
        INSERT INTO dds.settings (workflow_key, workflow_settings) 
        VALUES ('{}','{}');""".format(fetching_time,current_table)                    
    cur_1.execute(postgres_insert_query)    
    cur_1.execute(postgres_insert_query_settings)  
    
    conn_1.commit()
    conn_1.close()
    cur_1.close()

# Инициализация DDS
create_all_tables_dds = PostgresOperator(
    task_id="create_dds_tables",
    postgres_conn_id='PG_WAREHOUSE_CONNECTION',
    sql="./sql_scripts/create_dds_tables.sql",
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
        'load_dds_to_postgres',
        default_args=default_args,
        schedule_interval='0/15 * * * *',
        start_date=datetime(2024, 4, 22),
        catchup=False,
        tags=['sprint5', 'project'],
) as dag:

    # Создание логики DAG (последовательность/порядок)
    task1 = DummyOperator(task_id="start")
    with TaskGroup("load_dds_tables") as load_tables:
        task21 = PythonOperator(task_id="restaurants", python_callable=load_paste_data_restaurants, dag=dag)
        task22 = PythonOperator(task_id="couriers", python_callable=load_paste_data_couriers, dag=dag)
        task23 = PythonOperator(task_id="timestamps", python_callable=load_paste_data_timestamps, dag=dag)
        task24 = PythonOperator(task_id="orders", python_callable=load_paste_data_orders, dag=dag)
        task25 = PythonOperator(task_id="deliveries", python_callable=load_paste_data_deliveries, dag=dag)
    task4 = DummyOperator(task_id="end")
    
    task1 >> create_all_tables_dds >> load_tables >> task4


