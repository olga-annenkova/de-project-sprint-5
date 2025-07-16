import logging
import pendulum

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

log = logging.getLogger(__name__)

class DataLoader:
    def __init__(self, source_conn_id: str, target_conn_id: str, logger: logging.Logger):
        self.source_conn_id = source_conn_id
        self.target_conn_id = target_conn_id
        self.log = logger

    def load_table(self, source_table: str, target_table: str):
        self.log.info(f"Начинаем загрузку таблицы {source_table} из источника в {target_table} в DWH")

        source_hook = PostgresHook(postgres_conn_id=self.source_conn_id)
        target_hook = PostgresHook(postgres_conn_id=self.target_conn_id)

        source_conn = source_hook.get_conn()
        source_cursor = source_conn.cursor()
        source_cursor.execute(f"SELECT * FROM {source_table};")
        rows = source_cursor.fetchall()
        columns = [desc[0] for desc in source_cursor.description]
        self.log.info(f"Получено {len(rows)} строк из таблицы {source_table}")

        target_conn = target_hook.get_conn()
        target_cursor = target_conn.cursor()
        target_cursor.execute(f"TRUNCATE TABLE {target_table};")
        self.log.info(f"Таблица {target_table} очищена")

        placeholders = ', '.join(['%s'] * len(columns))
        insert_query = f"INSERT INTO {target_table} ({', '.join(columns)}) VALUES ({placeholders})"
        target_cursor.executemany(insert_query, rows)
        target_conn.commit()
        self.log.info(f"Данные успешно загружены в таблицу {target_table}")

        source_cursor.close()
        source_conn.close()
        target_cursor.close()
        target_conn.close()

class OutboxLoader:
    WF_KEY = "example_outbox_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"

    def __init__(self, source_conn_id: str, target_conn_id: str, logger: logging.Logger):
        self.source_conn_id = source_conn_id
        self.target_conn_id = target_conn_id
        self.log = logger

    def load_events(self):
        source_hook = PostgresHook(postgres_conn_id=self.source_conn_id)
        target_hook = PostgresHook(postgres_conn_id=self.target_conn_id)

        source_conn = source_hook.get_conn()
        source_cursor = source_conn.cursor()

        target_conn = target_hook.get_conn()
        target_cursor = target_conn.cursor()
        print("before try")
        try:
            # Получаем последний загруженный id из stg.srv_wf_settings
            
            target_cursor.execute(
                """
                SELECT workflow_settings->>%s AS last_loaded_id
                FROM stg.srv_wf_settings
                WHERE workflow_key = %s
                ORDER BY id DESC
                LIMIT 1
                """,
                (self.LAST_LOADED_ID_KEY, self.WF_KEY)
            )

            print("try")
            row = target_cursor.fetchone()
            print('load events: ', row)
            if row is None or len(row) == 0 or row[0] is None:
                last_loaded_id = 0
            else:
                last_loaded_id = int(row[0])
            # last_loaded_id = int(row[0]) if row and row[0] is not None else 0
            self.log.info(f"Last loaded outbox id: {last_loaded_id}")

            # Вычитываем новые записи из outbox
            source_cursor.execute(
                """
                SELECT * FROM outbox
                WHERE id > %s
                ORDER BY id ASC
                """,
                (last_loaded_id,)
            )
            rows = source_cursor.fetchall()
            if not rows:
                self.log.info("No new events to load from outbox.")
                return

            columns = [desc[0] for desc in source_cursor.description]
            self.log.info(f"Found {len(rows)} new events in outbox.")

            # Вставляем данные и обновляем курсор в одной транзакции
            with target_conn:
                with target_conn.cursor() as cur:
                    placeholders = ', '.join(['%s'] * len(columns))
                    insert_query = f"""
                        INSERT INTO stg.bonussystem_events ({', '.join(columns)}) VALUES ({placeholders})
                        ON CONFLICT (id) DO NOTHING
                    """
                    cur.executemany(insert_query, rows)

                    max_id = max(row[columns.index('id')] for row in rows)
                    import json
                    workflow_settings = json.dumps({self.LAST_LOADED_ID_KEY: max_id})

                    cur.execute(
                        """
                        INSERT INTO stg.srv_wf_settings (workflow_key, workflow_settings)
                        VALUES (%s, %s)
                        ON CONFLICT (workflow_key) DO UPDATE
                        SET workflow_settings = EXCLUDED.workflow_settings
                        """,
                        (self.WF_KEY, workflow_settings)
                    )
                    self.log.info(f"Loaded and saved progress up to id {max_id}.")

        finally:
            source_cursor.close()
            source_conn.close()
            target_cursor.close()
            target_conn.close()

@dag(
    schedule_interval='*/15 * * * *',
    start_date=pendulum.datetime(2025, 7, 2, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'stg', 'origin', 'example'],
    is_paused_upon_creation=True,
)
def stg_bonus_system_dag():
    source_conn_id = "PG_ORIGIN_BONUS_SYSTEM_CONNECTION"
    target_conn_id = "PG_WAREHOUSE_CONNECTION"
    loader = DataLoader(source_conn_id, target_conn_id, log)
    outbox_loader = OutboxLoader(source_conn_id, target_conn_id, log)

    @task(task_id="load_ranks")
    def load_ranks():
        loader.load_table("ranks", "stg.bonussystem_ranks")

    @task(task_id="load_users")
    def load_users():
        loader.load_table("users", "stg.bonussystem_users")

    @task(task_id="events_load")
    def events_load():
        outbox_loader.load_events()

    ranks_task = load_ranks()
    users_task = load_users()
    events_task = events_load()

    ranks_task >> users_task >> events_task

stg_bonus_system_dag_instance = stg_bonus_system_dag()

