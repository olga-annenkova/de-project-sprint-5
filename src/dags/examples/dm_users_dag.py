import logging
import pendulum
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from lib.dict_util import str2json  # Используем вашу функцию

log = logging.getLogger(__name__)

@dag(
    dag_id='dds_dm_users_load',
    schedule_interval='0/15 * * * *',  # Запуск каждый день, можно изменить
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=['dds', 'users', 'etl'],
    is_paused_upon_creation=False
)
def dds_dm_users_etl_dag():

    @task()
    def load_dm_users():
        log.info("Начинаем загрузку данных в dds.dm_users из stg.ordersystem_users")

        stg_conn = None
        stg_cursor = None
        dwh_conn = None
        dwh_cursor = None

        try:
            # Если STG и DDS в одной базе, используйте один и тот же conn_id
            stg_hook = PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION')
            dwh_hook = PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION')

            stg_conn = stg_hook.get_conn()
            stg_cursor = stg_conn.cursor()

            dwh_conn = dwh_hook.get_conn()
            dwh_cursor = dwh_conn.cursor()

            # Получаем только последние (актуальные) версии пользователей
            stg_cursor.execute("""
                SELECT object_id, object_value
                FROM (
                    SELECT
                        object_id,
                        object_value,
                        ROW_NUMBER() OVER (PARTITION BY object_id ORDER BY update_ts DESC) as rn
                    FROM stg.ordersystem_users
                ) AS sub
                WHERE sub.rn = 1;
            """)
            stg_rows = stg_cursor.fetchall()
            log.info(f"Получено {len(stg_rows)} уникальных пользователей")

            users_to_insert = []
            for object_id, object_value in stg_rows:
                try:
                    user_data = str2json(object_value)
                    user_name = user_data.get('name')
                    user_login = user_data.get('login')
                    if not object_id or not user_name or not user_login:
                        log.warning(f"Пропущен пользователь с неполными данными: {object_id}, {user_name}, {user_login}")
                        continue
                    users_to_insert.append((object_id, user_name, user_login))
                except Exception as e:
                    log.error(f"Ошибка при обработке пользователя {object_id}: {e}")

            if users_to_insert:
                insert_query = """
                    INSERT INTO dds.dm_users (user_id, user_name, user_login)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (user_id) DO UPDATE
                    SET user_name = EXCLUDED.user_name,
                        user_login = EXCLUDED.user_login;
                """
                dwh_cursor.executemany(insert_query, users_to_insert)
                dwh_conn.commit()
                log.info(f"Загружено/обновлено {len(users_to_insert)} пользователей в dds.dm_users")
            else:
                log.info("Нет пользователей для загрузки.")

        except Exception as e:
            log.error(f"Ошибка в процессе загрузки dm_users: {e}", exc_info=True)
            if dwh_conn:
                dwh_conn.rollback()
            raise
        finally:
            if stg_cursor is not None:
                stg_cursor.close()
            if stg_conn is not None:
                stg_conn.close()
            if dwh_cursor is not None:
                dwh_cursor.close()
            if dwh_conn is not None:
                dwh_conn.close()

    load_dm_users()

dds_dm_users_etl_dag = dds_dm_users_etl_dag()
