import logging
import pendulum
import json
from datetime import datetime
from typing import List, Dict

from airflow.decorators import dag, task
from airflow.models.variable import Variable

from lib import ConnectionBuilder, MongoConnect, PgConnect
from examples.stg.order_system_restaurants_dag.pg_saver import PgSaver
from examples.stg import EtlSetting, StgEtlSettingsRepository

log = logging.getLogger(__name__)

def datetime_to_str(obj):
    if isinstance(obj, dict):
        return {k: datetime_to_str(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [datetime_to_str(i) for i in obj]
    elif isinstance(obj, datetime):
        return obj.isoformat()
    else:
        return obj

class UserReader:
    def __init__(self, mc: MongoConnect) -> None:
        self.db = mc.get_db()

    def get_users(self, load_threshold: datetime, limit: int) -> List[Dict]:
        filter = {'update_ts': {'$gt': load_threshold}}
        sort = [('update_ts', 1)]
        docs = list(self.db.get_collection("users").find(filter=filter, sort=sort, limit=limit))
        print(docs)
        return docs

class PgSaver:
    def save_user(self, conn: PgConnect, id: str, update_ts: datetime, val: dict):
        print(val)
        user_id = str(val["_id"])
        val["_id"] = user_id
        val = datetime_to_str(val)
        str_val = json.dumps(val)  # Можно заменить на json2str, если есть
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO stg.ordersystem_users(object_id, object_value, update_ts)
                VALUES (%(id)s, %(val)s, %(update_ts)s)
                ON CONFLICT (object_id) DO UPDATE
                SET
                    object_value = EXCLUDED.object_value,
                    update_ts = EXCLUDED.update_ts;
                """,
                {
                    "id": id,
                    "val": str_val,
                    "update_ts": update_ts
                }
            )

class UserLoader:
    _LOG_THRESHOLD = 10
    _SESSION_LIMIT = 10000

    WF_KEY = "example_ordersystem_users_origin_to_stg_workflow"
    LAST_LOADED_TS_KEY = "last_loaded_ts"

    def __init__(self, user_reader: UserReader, pg_dest: PgConnect, pg_saver: PgSaver, logger: logging.Logger) -> None:
        self.user_reader = user_reader
        self.pg_saver = pg_saver
        self.pg_dest = pg_dest
        self.settings_repository = StgEtlSettingsRepository()
        self.log = logger

    def run_copy(self) -> int:
        with self.pg_dest.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(
                    id=0,
                    workflow_key=self.WF_KEY,
                    workflow_settings={
                        self.LAST_LOADED_TS_KEY: datetime(2022, 1, 1).isoformat()
                    }
                )

            last_loaded_ts_str = wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY]
            print(last_loaded_ts_str)
            last_loaded_ts = datetime.fromisoformat(last_loaded_ts_str)
            self.log.info(f"Starting load from last checkpoint: {last_loaded_ts}")

            load_queue = self.user_reader.get_users(last_loaded_ts, self._SESSION_LIMIT)
            self.log.info(f"Found {len(load_queue)} users to sync.")

            if not load_queue:
                self.log.info("No new users to load. Exiting.")
                return 0

            for i, user_doc in enumerate(load_queue, start=1):
                user_id = str(user_doc["_id"])
                update_ts = user_doc["update_ts"]
                self.pg_saver.save_user(conn, user_id, update_ts, user_doc)
                if i % self._LOG_THRESHOLD == 0:
                    self.log.info(f"Processed {i} of {len(load_queue)} users.")

            max_update_ts = max(user["update_ts"] for user in load_queue)
            wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY] = max_update_ts.isoformat()
            wf_setting_json = json.dumps(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Finished loading users. Last checkpoint updated to: {wf_setting_json}")

            return len(load_queue)

@dag(
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'example', 'stg', 'users'],
    is_paused_upon_creation=False,
)
def sprint5_example_stg_order_system_users():
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    cert_path = Variable.get("MONGO_DB_CERTIFICATE_PATH")
    db_user = Variable.get("MONGO_DB_USER")
    db_pw = Variable.get("MONGO_DB_PASSWORD")
    rs = Variable.get("MONGO_DB_REPLICA_SET")
    db = Variable.get("MONGO_DB_DATABASE_NAME")
    host = Variable.get("MONGO_DB_HOST")

    @task()
    def load_users_task():
        pg_saver = PgSaver()
        mongo_connect = MongoConnect(cert_path, db_user, db_pw, host, rs, db, db)
        user_reader = UserReader(mongo_connect)
        user_loader = UserLoader(user_reader, dwh_pg_connect, pg_saver, log)
        user_loader.run_copy()

    load_users_task()

sprint5_example_stg_order_system_users_dag = sprint5_example_stg_order_system_users()
