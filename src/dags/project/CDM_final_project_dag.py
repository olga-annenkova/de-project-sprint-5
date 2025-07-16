import logging
import pendulum
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

log = logging.getLogger(__name__)

@dag(
    schedule_interval='@daily',  # раз в день //для 15 минут '0/15 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"), 
    catchup=False,
    tags=['cdm', 'courier_ledger'],
)
def cdm_courier_ledger_dag():
    pg_conn_id = 'PG_WAREHOUSE_CONNECTION'
    pg_hook = PostgresHook(postgres_conn_id=pg_conn_id)

    @task
    def fill_dm_courier_ledger():
        log.info("Заполнение витрины cdm.dm_courier_ledger")
        with pg_hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO cdm.dm_courier_ledger (
                        courier_id,
                        courier_name,
                        settlement_year,
                        settlement_month,
                        orders_count,
                        orders_total_sum,
                        rate_avg,
                        order_processing_fee,
                        courier_order_sum,
                        courier_tips_sum,
                        courier_reward_sum
                    )
                    SELECT
                        d.courier_id,
                        c.courier_name,
                        EXTRACT(YEAR FROM d.delivery_ts)::int AS settlement_year,
                        EXTRACT(MONTH FROM d.delivery_ts)::int AS settlement_month,
                        COUNT(*) AS orders_count,
                        ROUND(SUM(d.sum), 2) AS orders_total_sum,
                        ROUND(AVG(d.rate), 2) AS rate_avg,
                        ROUND(SUM(d.sum) * 0.25, 2) AS order_processing_fee,
                        ROUND(
                            SUM(d.sum) * (
                                CASE
                                    WHEN AVG(d.rate) < 4      THEN 0.05
                                    WHEN AVG(d.rate) < 4.5    THEN 0.07
                                    WHEN AVG(d.rate) < 4.9    THEN 0.08
                                    ELSE 0.10
                                END
                            ), 2
                        ) AS courier_order_sum,
                        ROUND(SUM(d.tip_sum), 2) AS courier_tips_sum,
                        ROUND(
                            (
                                SUM(d.sum) * (
                                    CASE
                                        WHEN AVG(d.rate) < 4      THEN 0.05
                                        WHEN AVG(d.rate) < 4.5    THEN 0.07
                                        WHEN AVG(d.rate) < 4.9    THEN 0.08
                                        ELSE 0.10
                                    END
                                ) 
                                + SUM(d.tip_sum) * 0.95
                            ), 2
                        ) AS courier_reward_sum
                    FROM dds.dm_deliveries d
                    JOIN dds.dm_couriers c ON d.courier_id = c.courier_id
                    GROUP BY
                        d.courier_id, c.courier_name,
                        EXTRACT(YEAR FROM d.delivery_ts),
                        EXTRACT(MONTH FROM d.delivery_ts)
                    ON CONFLICT (courier_id, settlement_year, settlement_month)
                    DO UPDATE SET
                        courier_name = EXCLUDED.courier_name,
                        orders_count = EXCLUDED.orders_count,
                        orders_total_sum = EXCLUDED.orders_total_sum,
                        rate_avg = EXCLUDED.rate_avg,
                        order_processing_fee = EXCLUDED.order_processing_fee,
                        courier_order_sum = EXCLUDED.courier_order_sum,
                        courier_tips_sum = EXCLUDED.courier_tips_sum,
                        courier_reward_sum = EXCLUDED.courier_reward_sum;
                """)
            conn.commit()
        log.info("Витрина cdm.dm_courier_ledger обновлена")

    fill_dm_courier_ledger()

cdm_courier_ledger_dag_instance = cdm_courier_ledger_dag()

