import pendulum
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator


def ranks_extract ():
    df = PostgresHook(postgres_conn_id='PG_ORIGIN_BONUS_SYSTEM_CONNECTION').get_pandas_df(
    """
    select id, name, bonus_percent, min_payment_threshold
    from ranks;
    """)
    PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').insert_rows('stg.bonussystem_ranks', df.values, target_fields=df.columns.tolist(), commit_every=100, replace=True, replace_index='id')


def users_extract ():
    df = PostgresHook(postgres_conn_id='PG_ORIGIN_BONUS_SYSTEM_CONNECTION').get_pandas_df(
    """
    select id, order_user_id
    from users;
    """)
    PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').insert_rows('stg.bonussystem_users', df.values, target_fields=df.columns.tolist(), commit_every=100, replace=True, replace_index='id')


def events_extract ():
    last_id = PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').get_first(
    '''
    select coalesce(max(workflow_settings), '0') from stg.srv_wf_settings 
    where id = (
    select max(id)
    from stg.srv_wf_settings
    where workflow_key = 'outbox'
    );
    ''')

    df = PostgresHook(postgres_conn_id='PG_ORIGIN_BONUS_SYSTEM_CONNECTION').get_pandas_df(
    '''
    select id, event_ts, event_type, event_value from outbox where id > 0;
    ''')

    df_events = df[['event_ts', 'event_type', 'event_value']]
    PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').insert_rows('stg.bonussystem_events', df_events.values, target_fields=df_events.columns.tolist())
    df_id = df.id
    values = [['outbox', max(df_id)]]
    target_fields = ['workflow_key', 'workflow_settings']
    PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').insert_rows('stg.srv_wf_settings', rows=values, target_fields=target_fields)



with DAG(dag_id='bonus_system_stg', start_date=pendulum.datetime(2022, 5, 5, tz="UTC"), schedule_interval='0/15 * * * *', catchup=False) as dag:
    t1 = PythonOperator(
    task_id='ranks_extract',
    python_callable=ranks_extract
    )
    t2 = PythonOperator(
    task_id='users_extract',
    python_callable=users_extract
    )
    t3 = PythonOperator(
    task_id='events_extract',
    python_callable=events_extract
    )

t1 >> t2 >> t3