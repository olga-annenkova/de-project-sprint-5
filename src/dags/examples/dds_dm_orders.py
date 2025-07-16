import logging
import pendulum
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

log = logging.getLogger(__name__)

@dag(
    dag_id='dds_dm_orders_load',
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2025, 7, 6, tz="UTC"),
    catchup=False,
    tags=['dds', 'orders', 'etl'],
    is_paused_upon_creation=False
)
def dds_dm_orders_etl_dag():

    @task()
    def load_dm_orders():
        log.info("Начинаем загрузку данных в dds.dm_orders из stg.ordersystem_orders")

        pg_hook = PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        print('conn: ', conn)

        try:
            # Сначала проверим общее количество записей в источнике
            cursor.execute("SELECT COUNT(*) FROM stg.ordersystem_orders WHERE object_value IS NOT NULL")
            total_count = cursor.fetchone()[0]
            log.info(f"Всего записей в stg.ordersystem_orders: {total_count}")

            # Основной запрос для выборки данных
            query = """
                WITH orders_data AS (
                    SELECT
                        o.object_id AS order_key,
                        (o.object_value::jsonb ->> 'final_status') AS order_status,
                        (o.object_value::jsonb -> 'restaurant' ->> 'id') AS restaurant_source_id,
                        (o.object_value::jsonb -> 'user' ->> 'id') AS user_source_id,
                        (o.object_value::jsonb ->> 'date')::timestamp AS order_ts
                    FROM stg.ordersystem_orders o
                    WHERE (o.object_value::jsonb ->> 'date')::date BETWEEN (now() AT TIME ZONE 'utc')::date - 2 AND (now() AT TIME ZONE 'utc')::date - 1
                ),
                restaurant_data AS (
                    SELECT 
                        r.id AS restaurant_id,
                        r.restaurant_id AS restaurant_source_id,
                        r.active_from,
                        r.active_to
                    FROM dds.dm_restaurants r
                ),
                timestamp_data AS (
                    SELECT id, ts FROM dds.dm_timestamps
                ),
                user_data AS (
                    SELECT id, user_id FROM dds.dm_users
                )
                SELECT
                    od.order_key,
                    od.order_status,
                    ud.id AS user_id,
                    rd.restaurant_id,
                    td.id AS timestamp_id
                FROM orders_data od
                LEFT JOIN restaurant_data rd ON rd.restaurant_source_id = od.restaurant_source_id 
                LEFT JOIN user_data ud ON ud.user_id = od.user_source_id
                LEFT JOIN timestamp_data td ON td.ts = od.order_ts
              --  WHERE rd.restaurant_id IS NOT NULL
               -- AND ud.id IS NOT NULL
               -- AND td.id IS NOT NULL
               ;
            """

            cursor.execute(query)
            rows = cursor.fetchall()
            print('rows: ', rows)
            log.info(f"Получено {len(rows)} заказов для загрузки (из {total_count} в источнике)")

            if not rows:
                log.warning("Нет данных для загрузки")
                return

            # Пакетная вставка
            cursor.executemany("""
                INSERT INTO dds.dm_orders (
                    order_key,
                    order_status,
                    user_id,
                    restaurant_id,
                    timestamp_id
                )
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (order_key) DO UPDATE SET
                    order_status = EXCLUDED.order_status,
                    user_id = EXCLUDED.user_id,
                    restaurant_id = EXCLUDED.restaurant_id,
                    timestamp_id = EXCLUDED.timestamp_id;
            """, rows)
            
            conn.commit()
            log.info(f"Успешно вставлено/обновлено {len(rows)} записей")

            # Логирование статистики по незагруженным записям
            cursor.execute("""
                SELECT * FROM stg.ordersystem_orders
                WHERE object_value IS NOT NULL
                AND object_value::text <> 'null'
                AND object_value::text NOT IN ('{}', '[]')  -- исключаем пустые JSON-объекты и массивы, если нужно
                AND object_id NOT IN (
                    SELECT order_key FROM dds.dm_orders
                );
            """)
            not_loaded = cursor.fetchone()[0]
            print(not_loaded)
            # if not_loaded > 0:
            #     log.warning(f"Не загружено {not_loaded} записей (проблемы со связями)")

        except Exception as e:
            conn.rollback()
            log.error(f"Ошибка в процессе загрузки dm_orders: {e}", exc_info=True)
            raise
        finally:
            cursor.close()
            conn.close()

    load_dm_orders()

dds_dm_orders_etl_dag = dds_dm_orders_etl_dag()
