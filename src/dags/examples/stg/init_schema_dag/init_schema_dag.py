import logging
import pendulum
from airflow.decorators import dag, task
from airflow.models.variable import Variable
from lib import ConnectionBuilder
from airflow.providers.postgres.hooks.postgres import PostgresHook

log = logging.getLogger(__name__)

# Пример класса для загрузки таблицы users
class UsersLoader:
    def __init__(self, source_conn_id: str, target_conn_id: str, logger: logging.Logger):
        self.source_conn_id = source_conn_id
        self.target_conn_id = target_conn_id
        self.log = logger

    def load_users(self):
        self.log.info("Начинаем загрузку таблицы users из источника в DWH")

        source_hook = PostgresHook(postgres_conn_id=self.source_conn_id)
        target_hook = PostgresHook(postgres_conn_id=self.target_conn_id)

        source_conn = source_hook.get_conn()
        source_cursor = source_conn.cursor()
        source_cursor.execute("SELECT * FROM users;")
        rows = source_cursor.fetchall()
        columns = [desc[0] for desc in source_cursor.description]
        self.log.info(f"Получено {len(rows)} строк из таблицы users")

        target_conn = target_hook.get_conn()
        target_cursor = target_conn.cursor()
        target_cursor.execute("TRUNCATE TABLE stg.bonussystem_users;")
        self.log.info("Таблица stg.bonussystem_users очищена")

        placeholders = ', '.join(['%s'] * len(columns))
        insert_query = f"INSERT INTO stg.bonussystem_users ({', '.join(columns)}) VALUES ({placeholders})"
        target_cursor.executemany(insert_query, rows)
        target_conn.commit()
        self.log.info("Данные успешно загружены в stg.bonussystem_users")

        source_cursor.close()
        source_conn.close()
        target_cursor.close()
        target_conn.close()

@dag(
    schedule_interval='*/15 * * * *',  # Запуск каждые 15 минут
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'stg', 'schema', 'ddl', 'example'],
    is_paused_upon_creation=True
)
def sprint5_example_stg_init_schema_dag():
    # Подключение к базе DWH
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Путь к SQL-файлам для инициализации схемы
    ddl_path = Variable.get("EXAMPLE_STG_DDL_FILES_PATH")

    # Таск инициализации схемы
    @task(task_id="schema_init")
    def schema_init():
        from examples.stg.init_schema_dag.schema_init import SchemaDdl
        rest_loader = SchemaDdl(dwh_pg_connect, log)
        rest_loader.init_schema(ddl_path)

    # Таск загрузки таблицы users
    @task(task_id="load_users")
    def load_users():
        source_conn_id = "PG_ORIGIN_BONUS_SYSTEM_CONNECTION"
        target_conn_id = "PG_WAREHOUSE_CONNECTION"
        loader = UsersLoader(source_conn_id, target_conn_id, log)
        loader.load_users()

    # Инициализация тасков
    init_schema_task = schema_init()
    load_users_task = load_users()

    # Задаем порядок выполнения: сначала инициализация схемы, затем загрузка users
    init_schema_task >> load_users_task

stg_init_schema_dag = sprint5_example_stg_init_schema_dag()