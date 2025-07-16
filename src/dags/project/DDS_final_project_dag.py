import logging
import pendulum
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

log = logging.getLogger(__name__)

@dag(
    schedule_interval='@daily', # раз в день //для 15 минут '0/15 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['dds_load'],
)
def dds_layer_load_dag():
    pg_conn_id = 'PG_WAREHOUSE_CONNECTION'
    pg_hook = PostgresHook(postgres_conn_id=pg_conn_id)

    @task
    def load_dm_couriers():
        log.info("Загрузка dds.dm_couriers из stg.stg_couriers")
        with pg_hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO dds.dm_couriers (courier_id, courier_name)
                    SELECT 
                        (object_value::json->>'_id')::varchar AS courier_id,
                        (object_value::json->>'name')::varchar AS courier_name
                    FROM stg.stg_couriers
                    ON CONFLICT (courier_id) DO UPDATE 
                    SET courier_name = EXCLUDED.courier_name
                """)
            conn.commit()
        log.info("dds.dm_couriers обновлено")

    @task
    def load_dm_deliveries():
        log.info("Загрузка фактов dds.dm_deliveries из stg.stg_deliveries")
        with pg_hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                INSERT INTO dds.dm_deliveries (
                    order_id,
                    order_ts,
                    delivery_id,
                    courier_id,
                    address,
                    delivery_ts,
                    rate,
                    tip_sum,
                    sum
                )
                SELECT 
                    (object_value::json->>'order_id')::varchar,
                    (object_value::json->>'order_ts')::timestamp,
                    (object_value::json->>'delivery_id')::varchar,
                    (object_value::json->>'courier_id')::varchar,
                    (object_value::json->>'address')::varchar,
                    (object_value::json->>'delivery_ts')::timestamp,
                    COALESCE(NULLIF(object_value::json->>'rate','')::int,0),
                    COALESCE(NULLIF(object_value::json->>'tip_sum','')::numeric(14,2),0),
                    COALESCE(NULLIF(object_value::json->>'sum','')::numeric(14,2),0)
                FROM stg.stg_deliveries
                ON CONFLICT (delivery_id) DO UPDATE 
                SET 
                    order_id = EXCLUDED.order_id,
                    order_ts = EXCLUDED.order_ts,
                    courier_id = EXCLUDED.courier_id,
                    address = EXCLUDED.address,
                    delivery_ts = EXCLUDED.delivery_ts,
                    rate = EXCLUDED.rate,
                    tip_sum = EXCLUDED.tip_sum,
                    sum = EXCLUDED.sum
                """)
            conn.commit()
        log.info("dds.dm_deliveries обновлено")

    # сначала справочник, потом таблица фактов доставки
    load_dm_couriers() >> load_dm_deliveries()

dds_layer_load_dag_instance = dds_layer_load_dag()
