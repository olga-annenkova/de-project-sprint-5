import logging
import pendulum
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from lib.dict_util import str2json
from datetime import timedelta

log = logging.getLogger(__name__)

@dag(
    dag_id='dds_dm_restaurants_load',
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=['dds', 'restaurants', 'etl'],
    is_paused_upon_creation=False
)
def dds_dm_restaurants_etl_dag():

    @task()
    def load_dm_restaurants():
        log.info("Начинаем загрузку данных в dds.dm_restaurants из stg.ordersystem_restaurants")

        stg_hook = PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION')
        dwh_hook = PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION')

        stg_conn = stg_hook.get_conn()
        stg_cursor = stg_conn.cursor()

        dwh_conn = dwh_hook.get_conn()
        dwh_cursor = dwh_conn.cursor()

        try:
            # Получаем последние версии ресторанов
            stg_cursor.execute("""
                SELECT object_id, object_value, update_ts
                FROM (
                    SELECT
                        object_id,
                        object_value,
                        update_ts,
                        ROW_NUMBER() OVER (PARTITION BY object_id ORDER BY update_ts DESC) AS rn
                    FROM stg.ordersystem_restaurants
                ) sub
                WHERE sub.rn = 1;
            """)
            rows = stg_cursor.fetchall()
            log.info(f"Получено {len(rows)} уникальных ресторанов")

            # Получаем текущий max id в dm_restaurants
            dwh_cursor.execute("SELECT COALESCE(MAX(id), 0) FROM dds.dm_restaurants;")
            max_id = dwh_cursor.fetchone()[0]
            next_id = max_id + 1

            for object_id, object_value, update_ts in rows:
                try:
                    if isinstance(object_value, dict):
                        data = object_value
                    else:
                        data = str2json(object_value)

                    restaurant_name = data.get('name')

                    if not object_id or not restaurant_name or not update_ts:
                        log.warning(f"Пропущена запись с неполными данными: {object_id}, {restaurant_name}, {update_ts}")
                        continue

                    # Проверяем активную запись
                    dwh_cursor.execute("""
                        SELECT id, restaurant_name, active_from
                        FROM dds.dm_restaurants
                        WHERE restaurant_id = %s AND active_to = '2099-12-31 00:00:00'
                    """, (object_id,))
                    current_record = dwh_cursor.fetchone()

                    if current_record:
                        current_id, current_restaurant_name, current_active_from = current_record

                        if current_restaurant_name != restaurant_name or update_ts > current_active_from:
                            active_to_new = update_ts - timedelta(seconds=1)
                            dwh_cursor.execute("""
                                UPDATE dds.dm_restaurants
                                SET active_to = %s
                                WHERE id = %s
                            """, (active_to_new, current_id))
                            dwh_conn.commit()

                            # Вставляем новую запись с новым id
                            dwh_cursor.execute("""
                                INSERT INTO dds.dm_restaurants (id, restaurant_id, restaurant_name, active_from, active_to)
                                VALUES (%s, %s, %s, %s, '2099-12-31 00:00:00')
                            """, (next_id, object_id, restaurant_name, update_ts))
                            dwh_conn.commit()
                            log.info(f"Обновлена запись ресторана {object_id} с id {next_id}")
                            next_id += 1
                        else:
                            log.info(f"Ресторан {object_id} без изменений")
                    else:
                        # Вставляем новую запись с новым id
                        dwh_cursor.execute("""
                            INSERT INTO dds.dm_restaurants (id, restaurant_id, restaurant_name, active_from, active_to)
                            VALUES (%s, %s, %s, %s, '2099-12-31 00:00:00')
                        """, (next_id, object_id, restaurant_name, update_ts))
                        dwh_conn.commit()
                        log.info(f"Вставлена новая запись ресторана {object_id} с id {next_id}")
                        next_id += 1

                except Exception as e:
                    dwh_conn.rollback()
                    log.error(f"Ошибка при обработке ресторана {object_id}: {e}", exc_info=True)

        except Exception as e:
            log.error(f"Ошибка в процессе загрузки dm_restaurants: {e}", exc_info=True)
            if dwh_conn:
                dwh_conn.rollback()
            raise
        finally:
            stg_cursor.close()
            stg_conn.close()
            dwh_cursor.close()
            dwh_conn.close()

    load_dm_restaurants()

dds_dm_restaurants_etl_dag = dds_dm_restaurants_etl_dag()

