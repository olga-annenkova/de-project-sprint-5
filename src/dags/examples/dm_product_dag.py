import logging
import pendulum
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

log = logging.getLogger(__name__)

@dag(
    dag_id='dds_dm_products_load_from_restaurants',
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2025, 7, 6, tz="UTC"),
    catchup=False,
    tags=['dds', 'products', 'etl'],
    is_paused_upon_creation=False
)
def dds_dm_products_etl_dag():

    @task()
    def load_dm_products():
        log.info("Начинаем загрузку продуктов из меню ресторанов в dds.dm_products")

        stg_hook = PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION')
        dwh_hook = PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION')

        stg_conn = stg_hook.get_conn()
        stg_cursor = stg_conn.cursor()

        dwh_conn = dwh_hook.get_conn()
        dwh_cursor = dwh_conn.cursor()

        try:
            # Получаем максимальный id для генерации новых id, если нет sequence
            dwh_cursor.execute("SELECT COALESCE(MAX(id), 0) FROM dds.dm_products;")
            max_id = dwh_cursor.fetchone()[0]
            next_id = max_id + 1

            stg_cursor.execute("""
            WITH restaurants_products AS (
            SELECT
                r.id AS restaurant_id,
                rest.object_value::jsonb AS obj,
                (rest.object_value::jsonb->>'update_ts')::timestamp AS update_ts
            FROM dds.dm_restaurants r
            JOIN stg.ordersystem_restaurants rest
                ON rest.object_value::jsonb->>'_id' = r.restaurant_id
            ),
            products_expanded AS (
            SELECT
                restaurant_id,
                update_ts,
                product->>'_id' AS product_id,
                product->>'name' AS product_name,
                (product->>'price')::numeric(14,2) AS product_price
            FROM restaurants_products,
            jsonb_array_elements(obj->'menu') AS product
            )
            SELECT DISTINCT ON (product_id)
            restaurant_id,
            update_ts,
            product_id,
            product_name,
            product_price
            FROM products_expanded
            ORDER BY product_id, update_ts DESC;
            """)

            rows = stg_cursor.fetchall()
            log.info(f"Получено {len(rows)} продуктов для загрузки")

            for row in rows:
                restaurant_id, active_from, product_id, product_name, product_price = row

                try:
                    dwh_cursor.execute("""
                        INSERT INTO dds.dm_products
                            (id, restaurant_id, product_id, product_name, product_price, active_from, active_to)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (product_id, restaurant_id) DO NOTHING
                    """, (
                        next_id,
                        restaurant_id,
                        product_id,
                        product_name,
                        product_price,
                        active_from,
                        '2099-12-31 00:00:00'
                    ))
                    dwh_conn.commit()
                    log.info(f"Вставлен продукт id={next_id}, product_id={product_id}")
                    next_id += 1
                except Exception as e:
                    dwh_conn.rollback()
                    log.error(f"Ошибка вставки продукта {product_id}: {e}", exc_info=True)

        except Exception as e:
            log.error(f"Ошибка загрузки продуктов: {e}", exc_info=True)
            if dwh_conn:
                dwh_conn.rollback()
            raise
        finally:
            stg_cursor.close()
            stg_conn.close()
            dwh_cursor.close()
            dwh_conn.close()

    load_dm_products()

dds_dm_products_etl_dag = dds_dm_products_etl_dag()
