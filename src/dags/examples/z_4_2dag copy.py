import logging
import pendulum
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

log = logging.getLogger(__name__)

@dag(
    schedule_interval='0/15 * * * *',  # запуск каждые 15 минут (можно изменить под ваши нужды)
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'example', 'load_ranks'],
    is_paused_upon_creation=False
)
def load_ranks_dag():

    @task()
    def load_ranks():
        log.info("Начинаем загрузку таблицы ranks из источника в DWH")

        source_hook = PostgresHook(postgres_conn_id='source_postgres')
        dwh_hook = PostgresHook(postgres_conn_id='dwh_postgres')

        # Получаем данные из source
        source_conn = source_hook.get_conn()
        source_cursor = source_conn.cursor()
        source_cursor.execute("SELECT * FROM ranks;")
        rows = source_cursor.fetchall()
        columns = [desc[0] for desc in source_cursor.description]
        log.info(f"Получено {len(rows)} строк из таблицы ranks")

        # Очищаем целевую таблицу в DWH
        dwh_conn = dwh_hook.get_conn()
        dwh_cursor = dwh_conn.cursor()
        dwh_cursor.execute("TRUNCATE TABLE stg.bonussystem_ranks;")
        log.info("Таблица stg.bonussystem_ranks очищена")

        # Вставляем данные в DWH
        placeholders = ', '.join(['%s'] * len(columns))
        insert_query = f"INSERT INTO stg.bonussystem_ranks ({', '.join(columns)}) VALUES ({placeholders})"
        dwh_cursor.executemany(insert_query, rows)
        dwh_conn.commit()
        log.info("Данные успешно загружены в stg.bonussystem_ranks")

        source_cursor.close()
        source_conn.close()
        dwh_cursor.close()
        dwh_conn.close()

    load_ranks()

load_ranks_dag = load_ranks_dag()

