import pendulum
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator


def dm_settlement_report ():
    PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').run(
    """
    INSERT INTO cdm.dm_settlement_report (restaurant_id, restaurant_name, settlement_date, orders_count, orders_total_sum, orders_bonus_payment_sum, orders_bonus_granted_sum, order_processing_fee, restaurant_reward_sum)
    SELECT 
        restaurant_id,
        restaurant_name,
        settlement_date,
        SUM(fps_count) as orders_count,
        SUM(total_sum) as orders_total_sum,
        SUM(bonus_payment) as orders_bonus_payment_sum,
        SUM(bonus_grant) as orders_bonus_granted_sum,
        SUM(order_processing_fee) as order_processing_fee,
        SUM(restaurant_reward) as restaurant_reward_sum
    FROM 
    (
        SELECT
            fps.id,
            dmr.id as restaurant_id,
            dmr.restaurant_name,
            dmt."date" as settlement_date,
            fps."count" as fps_count,
            fps.total_sum,
            fps.bonus_payment,
            fps.bonus_grant,
            ROUND(fps.total_sum*0.25,2) as order_processing_fee,
            (fps.total_sum - ROUND(fps.total_sum*0.25,2) - fps.bonus_payment) as restaurant_reward
        FROM dds.fct_product_sales fps
        LEFT JOIN dds.dm_orders dmo ON dmo.id = fps.order_id
        LEFT JOIN dds.dm_restaurants dmr ON dmr.id = dmo.restaurant_id
        LEFT JOIN dds.dm_timestamps dmt ON dmt.id = dmo.timestamp_id
        WHERE dmo.order_status = 'CLOSED'
    )
    GROUP BY restaurant_id, restaurant_name, settlement_date
    ON CONFLICT ON CONSTRAINT dm_settlement_report_restaurant_and_date_uindex DO UPDATE 
    SET 
        orders_count = EXCLUDED.orders_count, 
        orders_total_sum = EXCLUDED.orders_total_sum, 
        orders_bonus_payment_sum = EXCLUDED.orders_bonus_payment_sum, 
        orders_bonus_granted_sum = EXCLUDED.orders_bonus_granted_sum, 
        order_processing_fee = EXCLUDED.order_processing_fee, 
        restaurant_reward_sum = EXCLUDED.restaurant_reward_sum;
    """)

with DAG(dag_id='dds_to_cdm', start_date=pendulum.datetime(2022, 5, 5, tz="UTC"), schedule_interval='0/15 * * * *', catchup=False) as dag:
    t1 = PythonOperator(
    task_id='dm_settlement_report',
    python_callable=dm_settlement_report 
    ) 

t1 
