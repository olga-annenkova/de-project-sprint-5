# import logging
# import pendulum

# from airflow.decorators import dag, task
# from airflow.providers.postgres.hooks.postgres import PostgresHook

# log = logging.getLogger(__name__)

# class DataLoader:
#     def __init__(self, source_conn_id: str, target_conn_id: str, logger: logging.Logger):
#         self.source_conn_id = source_conn_id
#         self.target_conn_id = target_conn_id
#         self.log = logger

#     def load_table(self, source_table: str, target_table: str):
#         self.log.info(f"Начинаем загрузку таблицы {source_table} из источника в {target_table} в DWH")

#         source_hook = PostgresHook(postgres_conn_id=self.source_conn_id)
#         target_hook = PostgresHook(postgres_conn_id=self.target_conn_id)

#         # Получаем данные из исходной таблицы
#         source_conn = source_hook.get_conn()
#         source_cursor = source_conn.cursor()
#         source_cursor.execute(f"SELECT * FROM {source_table};")
#         rows = source_cursor.fetchall()
#         columns = [desc[0] for desc in source_cursor.description]
#         self.log.info(f"Получено {len(rows)} строк из таблицы {source_table}")

#         # Очищаем целевую таблицу в DWH
#         target_conn = target_hook.get_conn()
#         target_cursor = target_conn.cursor()
#         target_cursor.execute(f"TRUNCATE TABLE {target_table};")
#         self.log.info(f"Таблица {target_table} очищена")

#         # Вставляем данные в целевую таблицу
#         placeholders = ', '.join(['%s'] * len(columns))
#         insert_query = f"INSERT INTO {target_table} ({', '.join(columns)}) VALUES ({placeholders})"
#         target_cursor.executemany(insert_query, rows)
#         target_conn.commit()
#         self.log.info(f"Данные успешно загружены в таблицу {target_table}")

#         # Закрываем соединения
#         source_cursor.close()
#         source_conn.close()
#         target_cursor.close()
#         target_conn.close()

# @dag(
#     schedule_interval='*/15 * * * *',  # Запуск каждые 15 минут
#     start_date=pendulum.datetime(2025, 7, 2, tz="UTC"),
#     catchup=False,
#     tags=['sprint5', 'stg', 'origin', 'example'],
#     is_paused_upon_creation=True,
# )
# def stg_bonus_system_dag():
#     source_conn_id = "PG_ORIGIN_BONUS_SYSTEM_CONNECTION"
#     target_conn_id = "PG_WAREHOUSE_CONNECTION"
#     loader = DataLoader(source_conn_id, target_conn_id, log)

#     @task(task_id="load_ranks")
#     def load_ranks():
#         loader.load_table("ranks", "stg.bonussystem_ranks")

#     @task(task_id="load_users")
#     def load_users():
#         loader.load_table("users", "stg.bonussystem_users")

#     # Последовательное выполнение: сначала ranks, затем users
#     ranks_task = load_ranks()
#     users_task = load_users()

#     ranks_task >> users_task

# stg_bonus_system_dag_instance = stg_bonus_system_dag()
