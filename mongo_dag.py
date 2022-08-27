import pendulum
import json
import datetime
from bson.json_util import dumps
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator

from airflow.models.variable import Variable



from typing import List
from urllib.parse import quote_plus as quote
from pymongo.mongo_client import MongoClient


cert_path = Variable.get("MONGO_DB_CERTIFICATE_PATH")
db_user = Variable.get("MONGO_DB_USER")
db_pw = Variable.get("MONGO_DB_PASSWORD")
rs = Variable.get("MONGO_DB_REPLICA_SET")
db = Variable.get("MONGO_DB_DATABASE_NAME")
hosts = Variable.get("MONGO_DB_HOSTS")

# Заводим класс для подключения к MongoDB
class MongoConnect:
    def __init__(self,
                 cert_path: str,  # Путь до файла с сертификатом
                 user: str,  # Имя пользователя БД
                 pw: str,  # Пароль пользователя БД
                 hosts: str,  # Список хостов для подключения
                 rs: str,  # replica set.
                 auth_db: str,  # БД для аутентификации
                 main_db: str  # БД с данными
                 ) -> None:
        self.user = user
        self.pw = pw
        self.hosts = hosts
        self.replica_set = rs
        self.auth_db = auth_db
        self.main_db = main_db
        self.cert_path = cert_path

    # Формируем строку подключения к MongoDB
    def url(self) -> str:
        return 'mongodb://{user}:{pw}@{hosts}/?replicaSet={rs}&authSource={auth_src}'.format(
            user=quote(self.user),
            pw=quote(self.pw),
            hosts=self.hosts,
            rs=self.replica_set,
            auth_src=self.auth_db)

    # Создаём клиент к БД
    def client(self):
        return MongoClient(self.url(), tlsCAFile=self.cert_path)[self.main_db]

def mongo_extract (collection_name):
    # Создаём клиент к БД
    mongo_connect = MongoConnect(cert_path, db_user, db_pw, hosts, rs, db, db)
    dbs = mongo_connect.client()
    update_ts = PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').get_first(f"select coalesce(max(workflow_settings), '1980-01-01 00:00:00.000000') from stg.srv_wf_settings where workflow_key = '{collection_name}'")[0]
    # Объявляем параметры фильтрации
    update_ts = datetime.datetime.strptime(update_ts,"%Y-%m-%d %H:%M:%S.%f")
    filter = {'update_ts': {'$gt': update_ts}}
    # Объявляем параметры сортировки
    sort = [('update_ts', 1)]
    # Вычитываем документы из MongoDB с применением фильтра и сортировки
    docs = list(dbs.get_collection(collection_name).find(filter=filter, sort=sort, limit=1000))
    for doc in docs:
        values = [[str(doc['_id']), dumps(doc), doc['update_ts'].strftime("%Y-%m-%d %H:%M:%S.%f")]]
        target_fields = ['object_id', 'object_value', 'update_ts']
        PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').insert_rows('stg.ordersystem_'+collection_name, rows=values, target_fields=target_fields, commit_every=100, replace=True, replace_index='object_id')
        values = [[collection_name, doc['update_ts'].strftime("%Y-%m-%d %H:%M:%S.%f")]]
        target_fields = ['workflow_key', 'workflow_settings']
        PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').insert_rows('stg.srv_wf_settings', rows=values, target_fields=target_fields)


with DAG(dag_id='order_system_stg', start_date=pendulum.datetime(2022, 5, 5, tz="UTC"), schedule_interval='0/15 * * * *', catchup=False) as dag:
    t1 = PythonOperator(
    task_id='mongo_extract_restaurants',
    python_callable=mongo_extract,
    op_kwargs={'collection_name' : 'restaurants'}
    )
    t2 = PythonOperator(
    task_id='mongo_extract_users',
    python_callable=mongo_extract,
    op_kwargs={'collection_name' : 'users'}
    )
    t3 = PythonOperator(
    task_id='mongo_extract_orders',
    python_callable=mongo_extract,
    op_kwargs={'collection_name' : 'orders'}
    )

t1 >> t2 >> t3