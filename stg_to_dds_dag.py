import pendulum
import json
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator


target_fields_srv_wf_settings = ['workflow_key', 'workflow_settings']

def users_extract ():
    records = PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').get_records(
    """
    select object_id, object_value::json->'name', object_value::json->'login', update_ts::timestamp(6) from stg.ordersystem_users
    where update_ts > (select coalesce(max(workflow_settings), '1980-01-01')::timestamp(6) from dds.srv_wf_settings where workflow_key = 'users') 
    """)
    max_update_ts = PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').get_first("""select coalesce(max(workflow_settings), '1980-01-01')::timestamp(6) from dds.srv_wf_settings where workflow_key = 'users'""")[0]
    for record in records:
        values = [[record[0], record[1], record[2]]]
        target_fields = ['user_id', 'user_name', 'user_login']
        PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').insert_rows('dds.dm_users', rows=values, target_fields=target_fields)
        if record[3] > max_update_ts:
            max_update_ts = record[3]
            PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').insert_rows('dds.srv_wf_settings', rows=[['users', max_update_ts]], target_fields=target_fields_srv_wf_settings)


def restaurants_extract ():
    records = PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').get_records(
    """
    select object_id, 
       object_value::json->'name',
       update_ts::timestamp(6),
       LEAD(update_ts,1,'2099-12-31') over (ORDER BY object_id)
       from stg.ordersystem_restaurants 
       where update_ts > (select coalesce(max(workflow_settings), '1980-01-01')::timestamp(6) from dds.srv_wf_settings where workflow_key = 'restaurants')
    """)
    max_update_ts = PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').get_first("""select coalesce(max(workflow_settings), '1980-01-01')::timestamp(6) from dds.srv_wf_settings where workflow_key = 'restaurants'""")[0]
    for record in records:
        values = [[record[0], record[1], record[2], record[3]]]
        target_fields = ['restaurant_id', 'restaurant_name', 'active_from', 'active_to']
        PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').insert_rows('dds.dm_restaurants', rows=values, target_fields=target_fields)
        if record[2] > max_update_ts:
            max_update_ts = record[2]
            PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').insert_rows('dds.srv_wf_settings', rows=[['restaurants', max_update_ts]], target_fields=target_fields_srv_wf_settings)
    

def products_extract ():
    records = PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').get_records(
    """
    with tmp as (select (object_value::json->'_id')->>'$oid' as restaurant_id, 
        (json_array_elements(object_value::json->'menu')::json->'_id')->>'$oid' as product_id,
        (json_array_elements(object_value::json->'menu')::json->>'name') as product_name,
        (json_array_elements(object_value::json->'menu')::json->>'price') as product_price,
        update_ts as active_from
        from stg.ordersystem_restaurants)
    select dr.id, t.product_id, t.product_name, t.product_price, t.active_from::timestamp(0), dr.active_to::date, t.active_from::timestamp(6) as update_ts from tmp t
    join dds.dm_restaurants dr
    on  t.restaurant_id=dr.restaurant_id and t.active_from=dr.active_from
    where t.active_from::timestamp(6) > (select coalesce(max(workflow_settings), '1980-01-01')::timestamp(6) from dds.srv_wf_settings where workflow_key = 'products')
    """)
    max_update_ts = PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').get_first("""select coalesce(max(workflow_settings), '1980-01-01')::timestamp(6) from dds.srv_wf_settings where workflow_key = 'products'""")[0]
    for record in records:
        values = [[record[0], record[1], record[2], record[3], record[4], record[5]]]
        target_fields = ['restaurant_id', 'product_id', 'product_name', 'product_price', 'active_from', 'active_to']
        PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').insert_rows('dds.dm_products', rows=values, target_fields=target_fields)
        if record[6] > max_update_ts:
            max_update_ts = record[6]
            PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').insert_rows('dds.srv_wf_settings', rows=[['products', max_update_ts]], target_fields=target_fields_srv_wf_settings)

def ts_extract ():
    records = PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').get_pandas_df(
    """
    with t as (
    select object_id, object_value::json->'final_status' as status,
    (object_value::json->'date'->>'$date')::timestamp(6) as update_ts,
    EXTRACT(YEAR FROM update_ts)::integer as year,
    EXTRACT(MONTH FROM update_ts)::integer as month,
    EXTRACT(DAY FROM update_ts)::integer as day,
    EXTRACT(HOUR FROM update_ts)::integer as hour,
    EXTRACT(MINUTE FROM update_ts)::integer as minute,
    EXTRACT(SECOND FROM update_ts)::integer as second
    from stg.ordersystem_orders)
    select update_ts, year, month, day, make_time (hour, minute, second) as time, make_date (year, month, day) as date from t
    where update_ts > (select coalesce(max(workflow_settings), '1980-01-01')::timestamp(6) from dds.srv_wf_settings where workflow_key = 'timestamps')
    """)
    target_fields = ['ts', 'year', 'month', 'day', 'time', 'date']
    PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').insert_rows('dds.dm_timestamps', rows=records.values, target_fields=target_fields)
    max_update_ts = records.max()[0]
    if len(records)>0:
        PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').insert_rows('dds.srv_wf_settings', rows=[['timestamps', max_update_ts]], target_fields=target_fields_srv_wf_settings)

def orders_extract ():
    records = PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').get_pandas_df(
    """
    with tmp as 
        (select object_id as order_key, 
        (object_value::json->'date'->>'$date')::timestamp(6) as upd_ts,
        object_value::json->'restaurant'->'id'->>'$oid' as rest_id,
        object_value::json->'user'->'id'->>'$oid' as u_id,
        object_value::json->>'final_status' as status,
        update_ts
        from stg.ordersystem_orders)
    select du.id as user_id, dr.id as restaurant_id, dt.id as timestamp_id, t.order_key, t.status, t.update_ts from tmp t
        left join dds.dm_restaurants dr 
        on t.rest_id=dr.restaurant_id 
        left join dds.dm_users du 
        on t.u_id=du.user_id 
        left join dds.dm_timestamps dt 
        on t.upd_ts=dt.ts
    where t.upd_ts>dr.active_from and t.upd_ts<dr.active_to and t.upd_ts>dr.active_from and t.upd_ts<dr.active_to and 
    t.update_ts > (select coalesce(max(workflow_settings), '1980-01-01')::timestamp(6) from dds.srv_wf_settings where workflow_key = 'orders')
    """)
    max_update_ts = records.max()[5]
    records.drop(records.columns[5], axis=1, inplace=True)
    target_fields = ['user_id', 'restaurant_id', 'timestamp_id', 'order_key', 'order_status']
    PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').insert_rows('dds.dm_orders', rows=records.values, target_fields=target_fields)
    if len(records)>0:
        PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').insert_rows('dds.srv_wf_settings', rows=[['orders', max_update_ts]], target_fields=target_fields_srv_wf_settings)

def generate_fct_product_sales ():
    records = PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').get_pandas_df(
    """
    SELECT
        dp.id as product_id,
        ddo.id as order_id,
        q."count",
        q.price,
        q.total_sum,
        q.bonus_payment,
        q.bonus_grant,
        event_ts
    FROM
    (
    SELECT 
        json_array_elements(sbe.event_value::json->'product_payments')->>'product_id' AS product_id,	
        sbe.event_value::json->>'order_id' AS order_id,
        (json_array_elements(sbe.event_value::json->'product_payments')->>'quantity')::numeric AS "count",
        (json_array_elements(sbe.event_value::json->'product_payments')->>'price')::numeric AS price,
        (json_array_elements(sbe.event_value::json->'product_payments')->>'product_cost')::numeric AS total_sum,
        (json_array_elements(sbe.event_value::json->'product_payments')->>'bonus_payment')::numeric AS bonus_payment,
        (json_array_elements(sbe.event_value::json->'product_payments')->>'bonus_grant')::numeric AS bonus_grant,
        sbe.event_ts::timestamp AS event_ts
    FROM stg.bonussystem_events sbe
    WHERE sbe.event_type='bonus_transaction'
    ) AS q
    LEFT JOIN dds.dm_products dp ON dp.product_id = q.product_id AND dp.active_to > q.event_ts AND dp.active_from < q.event_ts
    LEFT JOIN dds.dm_orders ddo ON ddo.order_key = q.order_id
    WHERE q.event_ts > 
    (
    SELECT 
    coalesce(max(workflow_settings), '1980-01-01')::timestamp(6) 
    from dds.srv_wf_settings 
    where workflow_key ='fct_product_sales' )
    """)
    max_update_ts = records.max()[7]
    records.drop(records.columns[7], axis=1, inplace=True)
    target_fields = ['product_id', 'order_id', 'count', 'price', 'total_sum', 'bonus_payment', 'bonus_grant']
    PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').insert_rows('dds.fct_product_sales', rows=records.values, target_fields=target_fields)
    if len(records)>0:
        PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').insert_rows('dds.srv_wf_settings', rows=[['fct_product_sales', max_update_ts]], target_fields=target_fields_srv_wf_settings)


with DAG(dag_id='stg_to_dds', start_date=pendulum.datetime(2022, 5, 5, tz="UTC"), schedule_interval='0/15 * * * *', catchup=False) as dag:
    t1 = PythonOperator(
    task_id='users_extract',
    python_callable=users_extract 
    ) 
    t2 = PythonOperator(
    task_id='restaurants_extract',
    python_callable=restaurants_extract
    )
    t3 = PythonOperator(
    task_id='ts_extract',
    python_callable=ts_extract
    )
    t4 = PythonOperator(
    task_id='products_extract',
    python_callable=products_extract
    )
    t5 = PythonOperator(
    task_id='orders_extract',
    python_callable=orders_extract
    )
    t6 = PythonOperator(
    task_id='generate_fct_product_sales',
    python_callable=generate_fct_product_sales
    ) 

t1 >> t2 >> t4 >> t3 >> t5 >> t6
