import logging
import pendulum
from datetime import datetime
from airflow.decorators import dag, task
from airflow.models import Variable
from lib import ConnectionBuilder, MongoConnect
from pymongo.mongo_client import MongoClient
from psycopg2.extras import execute_batch
import json

log = logging.getLogger(__name__)

global counter

counter = 0

def datetime_to_str(obj):
    if isinstance(obj, dict):
        return {k: datetime_to_str(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [datetime_to_str(i) for i in obj]
    elif isinstance(obj, datetime):
        return obj.isoformat()
    else:
        return obj
def order_items_id_transfer(item):
    item['id'] = str(item['id'])
    return item
    
def orders_objectid_transform(order):
    global counter
    while counter < 1:
        print('orders_objectid_transform: ', order)
        counter += 1
    order_id = str(order["_id"])
    order["_id"] = order_id
    order = datetime_to_str(order)
    order['restaurant']['id'] = str(order['restaurant']['id'])
    order['user']['id'] = str(order['user']['id'])
    order['order_items'] = list(map(order_items_id_transfer, order['order_items']))
    # order_str = json.dumps(order)
    return order

@dag(
    schedule_interval='0/15 * * * *',  # каждые 15 минут
    start_date=pendulum.datetime(2022, 1, 1, tz="UTC"),
    catchup=False,
    tags=['stg', 'orders', 'load'],
    is_paused_upon_creation=False
)
def load_orders_from_mongo_to_pg():
    @task()
    def extract_orders_from_mongo():
        Variable.delete("last_orders_load_ts")
        """Извлекает заказы из MongoDB"""
        # Получаем настройки подключения из переменных Airflow
        mongo_conn = {
            'host': Variable.get("MONGO_DB_HOST"),
            'username': Variable.get("MONGO_DB_USER"),
            'password': Variable.get("MONGO_DB_PASSWORD"),
            'authSource': Variable.get("MONGO_DB_DATABASE_NAME"),
            'replicaSet': Variable.get("MONGO_DB_REPLICA_SET"),
            'tls': True,
            'tlsCAFile': Variable.get("MONGO_DB_CERTIFICATE_PATH")
        }
        
        # Подключаемся к MongoDB
        client = MongoClient(**mongo_conn)
        db = client[Variable.get("MONGO_DB_DATABASE_NAME")]
        
        # Получаем последнюю дату загрузки
        last_loaded = Variable.get("last_orders_load_ts", default_var="2022-01-01T00:00:00")
        last_loaded_ts = datetime.fromisoformat(last_loaded)
        
        # Извлекаем новые заказы
        orders = list(db.orders.find(
            {"update_ts": {"$gt": last_loaded_ts}},
            sort=[("update_ts", 1)]  # Сортируем по дате обновления
        ))
        orders = list(map(orders_objectid_transform, orders))
        print('orders: ', orders)

        client.close()
        return orders

    @task()
    def transform_and_load_to_pg(orders):
        """Загружает заказы в PostgreSQL"""
        if not orders:
            log.info("Нет новых заказов для загрузки")
            return
        
        # Подключаемся к PostgreSQL
        
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        pg_hook = PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION')
        conn = pg_hook.get_conn()
        
        try:
            with conn.cursor() as cursor:
                # Подготавливаем данные для вставки
                print('orders: ', orders[0])
                print('cursor: ', cursor)
                data = [(
                    str(order["_id"]),  # object_id
                    json.dumps(order),  # object_value (уже в JSON)
                    order["update_ts"]  # update_ts
                ) for order in orders]
                
                print('data: ', data)
                print('data length: ', len(data))
                # Вставляем данные пачками
                #data = list(data[0])
                execute_batch(
                    cursor,
                    """
                    INSERT INTO stg.ordersystem_orders(object_id, object_value, update_ts)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (object_id) DO UPDATE
                    SET
                        object_value = EXCLUDED.object_value,
                        update_ts = EXCLUDED.update_ts;
                    """,
                    data
                )
                conn.commit()
                
                # Обновляем переменную с последней датой загрузки
                last_update_ts = max(order["update_ts"] for order in orders)
                Variable.set("last_orders_load_ts", last_update_ts)
                
                log.info(f"Успешно загружено {len(orders)} заказов")
                
                
        finally:
            cursor.close()
            conn.close()
            conn = pg_hook.get_conn()
            try:
                with conn.cursor() as cursor:
                    cursor.execute('''SELECT count(*) FROM stg.ordersystem_orders;''')
                    rows = cursor.fetchall()
                    print('rows: ', rows)
            finally:
                print('end')


    # Оркестрация задач
    orders = extract_orders_from_mongo()
    transform_and_load_to_pg(orders)

load_orders_dag = load_orders_from_mongo_to_pg()