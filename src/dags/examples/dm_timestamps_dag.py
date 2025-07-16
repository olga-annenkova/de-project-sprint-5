import logging
import pendulum
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import timedelta

log = logging.getLogger(__name__)

@dag(
    dag_id='dds_dm_timestamps_load',
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2025, 7, 6, tz="UTC"),
    catchup=False,
    tags=['dds', 'timestamps', 'etl'],
    is_paused_upon_creation=False
)
def dds_dm_timestamps_etl_dag():

    @task()
    def load_dm_timestamps():
        log.info("Начинаем загрузку данных в dds.dm_timestamps из stg.ordersystem_orders")

        stg_hook = PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION')
        dwh_hook = PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION')

        stg_conn = stg_hook.get_conn()
        stg_cursor = stg_conn.cursor()

        dwh_conn = dwh_hook.get_conn()
        dwh_cursor = dwh_conn.cursor()

        try:
            stg_cursor.execute("""
            SELECT DISTINCT ON (final_status_date)
                order_id,
                final_status_date
            FROM (
                SELECT
                    o.object_id AS order_id,
                    (o.object_value::jsonb ->> 'date')::timestamp AS final_status_date
                FROM stg.ordersystem_orders o
                WHERE (o.object_value::jsonb ->> 'date') IS NOT NULL
            ) sub
            ORDER BY final_status_date, order_id;
            """)
            rows = stg_cursor.fetchall()
            print('rows: ', rows[0])
            log.info(f"Получено {len(rows)} записей с финальными статусами")

            dwh_cursor.execute("SELECT COALESCE(MAX(id), 0) FROM dds.dm_timestamps;")
            max_id = dwh_cursor.fetchone()[0]
            next_id = max_id + 1

            for order_id, final_status_date in rows:
                if not final_status_date:
                    log.warning(f"Пропущен заказ {order_id} без даты финального статуса")
                    continue

                try:
                    ts = final_status_date
                    year = ts.year
                    month = ts.month
                    day = ts.day
                    date_only = ts.date()
                    time_only = ts.time()

                    dwh_cursor.execute("""
                        INSERT INTO dds.dm_timestamps (id, ts, year, month, day, date, time)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """, (next_id, ts, year, month, day, date_only, time_only))
                    dwh_conn.commit()
                    log.info(f"Вставлена запись id={next_id} для заказа {order_id} с датой {ts}")
                    next_id += 1

                except Exception as e:
                    dwh_conn.rollback()
                    log.error(f"Ошибка при вставке записи для заказа {order_id}: {e}", exc_info=True)

        except Exception as e:
            log.error(f"Ошибка в процессе загрузки dm_timestamps: {e}", exc_info=True)
            if dwh_conn:
                dwh_conn.rollback()
            raise
        finally:
            stg_cursor.close()
            stg_conn.close()
            dwh_cursor.close()
            dwh_conn.close()

    load_dm_timestamps()

dds_dm_timestamps_etl_dag = dds_dm_timestamps_etl_dag()

