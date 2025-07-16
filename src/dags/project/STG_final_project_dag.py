import logging
import pendulum
import requests
import json
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook


class ApiToStgLoader:
    def __init__(self, pg_conn_id: str, logger: logging.Logger):
        self.pg_conn_id = pg_conn_id
        self.log = logger

    # универсальный метод для курьеров, доставки и ресторанов
    def load_api_data(self, entity_name: str, api_endpoint: str, sort_field: str, limit: int = 50):
        offset = 0 # 
        total_loaded = 0 # Счётчик успешно загруженных записей

        pg_hook = PostgresHook(postgres_conn_id=self.pg_conn_id)


        now = datetime.now() # сегодня
        # и семь дней назад
        seven_days_ago = (now - timedelta(days=7)).strftime('%Y-%m-%d %H:%M:%S')
        now_str = now.strftime('%Y-%m-%d %H:%M:%S')

        HEADERS = {
            "X-API-KEY": "25c27781-8fde-4b30-a22e-524044a7580f",
            "X-Nickname": "olga",
            "X-Cohort": "38"
        }

        # пиводим в соответствие
        id_field_map = {
            'couriers': '_id',
            'restaurants': '_id',
            'deliveries': 'delivery_id'
        }

        id_field = id_field_map.get(entity_name, '_id')

        # пока API отдаёт данные
        while True:
            params = {
                'limit': limit,
                'offset': offset,
                'sort_field': sort_field,
                'sort_direction': 'asc'
            }
            if entity_name == 'deliveries':
                params['from'] = seven_days_ago
                params['to'] = now_str

            url = f'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/{api_endpoint}'

            self.log.info(f"Запрос к {url} с параметрами {params}")
            response = requests.get(url, headers=HEADERS, params=params)
            response.raise_for_status()
            records = response.json()

            # все контролируем в логах
            self.log.info(f"Получено {len(records)} записей для {entity_name} по offset {offset}")

            if not records:
                self.log.info(f"Данных больше нет для {entity_name}, всего загружено: {total_loaded}")
                break

            data_to_insert = []
            for rec in records:
                obj_id = rec.get(id_field)
                if obj_id is None:
                    self.log.warning(f"Пропущена запись без уникального ключа {id_field}: {rec}")
                    continue
                obj_json = json.dumps(rec, ensure_ascii=False)
                data_to_insert.append((obj_id, obj_json))

            self.log.info(entity_name)

            self.log.info(f"Готово к вставке {len(data_to_insert)} записей для {entity_name}")

            # можно вставлять в таблицу
            insert_sql = f"""
                INSERT INTO stg.stg_{entity_name} (object_id, object_value, loaded_at)
                VALUES (%s, %s, now())
                ON CONFLICT (object_id) DO NOTHING
            """
            self.log.info(f"Готово к вставке {insert_sql } ")
            with pg_hook.get_conn() as conn:
                with conn.cursor() as cur:
                    cur.executemany(insert_sql, data_to_insert)
                conn.commit()


            self.log.info(f"Вставлено {len(data_to_insert)} записей в stg.{entity_name}")

            offset += limit
            total_loaded += len(data_to_insert)


@dag(
    schedule_interval='@daily', # раз в день //для 15 минут '0/15 * * * *',
    start_date=pendulum.datetime(2025, 7, 15, tz="UTC"),
    catchup=False,
    tags=['stg_api_load'],
)
def stg_api_load_dag():

    pg_conn_id = 'PG_WAREHOUSE_CONNECTION'
    api_loader = ApiToStgLoader(pg_conn_id, logging.getLogger(__name__))

    @task(task_id="load_couriers_to_stg")
    def load_couriers_task():
        api_loader.load_api_data('couriers', 'couriers', 'id')

    @task(task_id="load_deliveries_to_stg")
    def load_deliveries_task():
        api_loader.load_api_data('deliveries', 'deliveries', 'date')

    @task(task_id="load_restaurants_to_stg")
    def load_restaurants_task():
        api_loader.load_api_data('restaurants', 'restaurants', 'id')

    # запускаем таски: курьеры -> доставки -> рестораны 
    load_couriers_task() >> load_deliveries_task() >> load_restaurants_task()


stg_api_load_dag_instance = stg_api_load_dag()
