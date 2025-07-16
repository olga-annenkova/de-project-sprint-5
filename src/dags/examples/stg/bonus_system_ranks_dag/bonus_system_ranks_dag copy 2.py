# import logging
# import pendulum
# from airflow.decorators import dag, task
# from lib import ConnectionBuilder
# from airflow.providers.postgres.hooks.postgres import PostgresHook

# log = logging.getLogger(__name__)

# class RankLoader:
#     def __init__(self, source_conn, target_conn, logger):
#         self.source_conn = source_conn
#         self.target_conn = target_conn
#         self.log = logger

#     def load_ranks(self):
#         self.log.info("Начинаем загрузку таблицы ranks из источника в DWH")

#         source_hook = PostgresHook(postgres_conn_id=self.source_conn)
#         target_hook = PostgresHook(postgres_conn_id=self.target_conn)

#         # Получаем данные из source
#         source_conn = source_hook.get_conn()
#         source_cursor = source_conn.cursor()
#         source_cursor.execute("SELECT * FROM ranks;")
#         rows = source_cursor.fetchall()
#         columns = [desc[0] for desc in source_cursor.description]
#         self.log.info(f"Получено {len(rows)} строк из таблицы ranks")

#         # Очищаем целевую таблицу в DWH
#         target_conn = target_hook.get_conn()
#         target_cursor = target_conn.cursor()
#         target_cursor.execute("TRUNCATE TABLE stg.bonussystem_ranks;")
#         self.log.info("Таблица stg.bonussystem_ranks очищена")

#         # Вставляем данные в DWH
#         placeholders = ', '.join(['%s'] * len(columns))
#         insert_query = f"INSERT INTO stg.bonussystem_ranks ({', '.join(columns)}) VALUES ({placeholders})"
#         target_cursor.executemany(insert_query, rows)
#         target_conn.commit()
#         self.log.info("Данные успешно загружены в stg.bonussystem_ranks")

#         source_cursor.close()
#         source_conn.close()
#         target_cursor.close()
#         target_conn.close()

# @dag(
#     schedule_interval='*/15 * * * *',  # Каждые 15 минут
#     start_date=pendulum.datetime(2025, 7, 2, tz="UTC"),
#     catchup=False,
#     tags=['sprint5', 'stg', 'origin', 'example'],
#     is_paused_upon_creation=True
# )
# def sprint5_example_stg_bonus_system_ranks_dag():
#     # Создаем подключения к базам по именам коннекшенов Airflow
#     dwh_pg_connect = "PG_WAREHOUSE_CONNECTION"
#     origin_pg_connect = "PG_ORIGIN_BONUS_SYSTEM_CONNECTION"

#     @task(task_id="ranks_load")
#     def load_ranks():
#         loader = RankLoader(origin_pg_connect, dwh_pg_connect, log)
#         loader.load_ranks()

#     load_ranks()

# stg_bonus_system_ranks_dag = sprint5_example_stg_bonus_system_ranks_dag()

