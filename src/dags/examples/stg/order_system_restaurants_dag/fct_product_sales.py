import logging
import pendulum
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

log = logging.getLogger(__name__)

@dag(
    schedule_interval='0 1 * * *',  # Запускать каждый день в 01:00 UTC (можно изменить)
    start_date=pendulum.datetime(2022, 1, 1, tz="UTC"),
    catchup=False,
    tags=['dds', 'fct_product_sales', 'load'],
    is_paused_upon_creation=False
)
def load_fct_product_sales_dag():

    @task()
    def load_fct_product_sales():
        """
        Загружает данные в dds.fct_product_sales из dm_orders, dm_products и бонусной подсистемы.
        """
        pg_hook = PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION')
        conn = pg_hook.get_conn()

        try:
            with conn.cursor() as cursor:
                insert_sql = """
                INSERT INTO dds.fct_product_sales
                    (product_id, order_id, count, price, total_sum, bonus_payment, bonus_grant)
                SELECT 
                    dm_p.id AS product_id,
                    dm_o.id AS order_id,
                    --bp.order_id,
                    bp.count,
                    bp.price,
                    bp.total_sum,
                    bp.bonus_payment,
                    bp.bonus_grant
                FROM (        
                SELECT 
                    (pp->>'product_id')::varchar AS product_id,
                    (be.event_value::json->>'order_id')::varchar AS order_id,
                    (pp->>'quantity')::int AS count,
                    (pp->>'price')::numeric(19,5) AS price,
                    (pp->>'product_cost')::numeric(19,5) AS total_sum,
                    (pp->>'bonus_payment')::numeric(19,5) AS bonus_payment,
                    (pp->>'bonus_grant')::numeric(19,5) AS bonus_grant
                FROM stg.bonussystem_events AS be
                CROSS JOIN LATERAL json_array_elements((be.event_value::json->>'product_payments')::json) AS pp
                WHERE be.event_ts::date BETWEEN (now() AT TIME ZONE 'utc')::date - 2 AND (now() AT TIME ZONE 'utc')::date - 1 AND
                be.event_type = 'bonus_transaction' ) bp
                JOIN (
                    SELECT
                        od.id,
                        od.order_key, 
                        od.restaurant_id
                    FROM dds.dm_orders od
                    --LEFT JOIN dds.dm_timestamps dts ON od.timestamp_id = dts.id
                    LEFT JOIN dds.dm_restaurants dr ON od.restaurant_id = dr.id
                    --WHERE dts.ts::date BETWEEN (now() AT TIME ZONE 'utc')::date - 2 AND (now() AT TIME ZONE 'utc')::date - 1
                ) AS dm_o ON bp.order_id = dm_o.order_key
                LEFT JOIN (SELECT DISTINCT ON (product_id) id, product_id FROM dds.dm_products ORDER BY product_id, id) dm_p
                    ON dm_p.product_id = bp.product_id;
                """
                cursor.execute(insert_sql)
                conn.commit()
                log.info("dds.fct_product_sales успешно загружена")
        finally:
            conn.close()

    load_fct_product_sales()

load_fct_product_sales_dag = load_fct_product_sales_dag()
