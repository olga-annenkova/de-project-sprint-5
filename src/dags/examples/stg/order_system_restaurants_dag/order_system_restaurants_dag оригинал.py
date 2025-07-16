import logging
import pendulum
from datetime import datetime
from airflow.decorators import dag, task
from airflow.models.variable import Variable

from lib import ConnectionBuilder, MongoConnect
from examples.stg.order_system_restaurants_dag.pg_saver import PgSaver
from examples.stg.order_system_restaurants_dag.restaurant_reader import RestaurantReader
from examples.stg.order_system_restaurants_dag.restaurant_loader import RestaurantLoader

log = logging.getLogger(__name__)

@dag(
    schedule_interval='0/15 * * * *',  # каждые 15 минут
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'example', 'stg', 'origin'],
    is_paused_upon_creation=False
)
def sprint5_example_stg_order_system_restaurants():
    # Подключение к DWH (Postgres)
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Читаем переменные для подключения к MongoDB из Airflow Variables
    cert_path = Variable.get("MONGO_DB_CERTIFICATE_PATH")
    db_user = Variable.get("MONGO_DB_USER")
    db_pw = Variable.get("MONGO_DB_PASSWORD")
    rs = Variable.get("MONGO_DB_REPLICA_SET")
    db = Variable.get("MONGO_DB_DATABASE_NAME")
    host = Variable.get("MONGO_DB_HOST")

    @task()
    def load_restaurants():
        log.info("Запуск загрузки ресторанов из MongoDB")

        # Инициализация saver для сохранения в Postgres
        pg_saver = PgSaver()

        # Подключение к MongoDB
        mongo_connect = MongoConnect(cert_path, db_user, db_pw, host, rs, db, db)

        # Инициализация reader для чтения из MongoDB
        collection_reader = RestaurantReader(mongo_connect)
        print('collection_reader: ', collection_reader)

        # Инициализация loader, который управляет процессом загрузки
        loader = RestaurantLoader(collection_reader, dwh_pg_connect, pg_saver, log)
        print('loader: ', loader)

        # Запускаем копирование данных из Mongo в Postgres
        loader.run_copy()

        log.info("Загрузка ресторанов завершена")

    return load_restaurants()

order_stg_dag = sprint5_example_stg_order_system_restaurants()
