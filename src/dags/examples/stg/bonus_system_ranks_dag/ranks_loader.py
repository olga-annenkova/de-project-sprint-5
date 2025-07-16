# from logging import Logger
# from typing import List
# from psycopg import Connection
# from psycopg.rows import class_row
# from pydantic import BaseModel
# from lib import PgConnect
# from examples.stg import EtlSetting, StgEtlSettingsRepository
# from lib.dict_util import json2str


# class OutboxObj(BaseModel):
#     id: int
#     event_type: str
#     payload: str
#     created_at: str  # или datetime, в зависимости от схемы


# class OutboxOriginRepository:
#     def __init__(self, pg: PgConnect) -> None:
#         self._db = pg

#     def list_outbox(self, last_loaded_id: int, limit: int) -> List[OutboxObj]:
#         with self._db.client().cursor(row_factory=class_row(OutboxObj)) as cur:
#             cur.execute(
#                 """
#                 SELECT id, event_type, payload, created_at
#                 FROM outbox
#                 WHERE id > %(last_loaded_id)s
#                 ORDER BY id ASC
#                 LIMIT %(limit)s;
#                 """,
#                 {
#                     "last_loaded_id": last_loaded_id,
#                     "limit": limit,
#                 },
#             )
#             return cur.fetchall()


# class OutboxDestRepository:
#     def insert_event(self, conn: Connection, event: OutboxObj) -> None:
#         with conn.cursor() as cur:
#             cur.execute(
#                 """
#                 INSERT INTO stg.bonussystem_events (id, event_type, payload, created_at)
#                 VALUES (%(id)s, %(event_type)s, %(payload)s, %(created_at)s)
#                 ON CONFLICT (id) DO NOTHING;
#                 """,
#                 {
#                     "id": event.id,
#                     "event_type": event.event_type,
#                     "payload": event.payload,
#                     "created_at": event.created_at,
#                 },
#             )


# class OutboxLoader:
#     WF_KEY = "example_outbox_to_stg_workflow"
#     LAST_LOADED_ID_KEY = "last_loaded_id"
#     BATCH_LIMIT = 1000  # Можно увеличить пакет для оптимизации

#     def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
#         self.pg_dest = pg_dest
#         self.origin = OutboxOriginRepository(pg_origin)
#         self.stg = OutboxDestRepository()
#         self.settings_repository = StgEtlSettingsRepository()
#         self.log = log

#     def load_events(self):
#         with self.pg_dest.connection() as conn:
#             # Получаем состояние загрузки
#             wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
#             if not wf_setting:
#                 wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

#             last_loaded = wf_setting.workflow_settings.get(self.LAST_LOADED_ID_KEY, -1)
#             self.log.info(f"Last loaded outbox id: {last_loaded}")

#             # Получаем новую пачку событий
#             load_queue = self.origin.list_outbox(last_loaded, self.BATCH_LIMIT)
#             self.log.info(f"Found {len(load_queue)} new outbox events to load.")

#             if not load_queue:
#                 self.log.info("No new events to load. Exiting.")
#                 return

#             # Вставляем события в целевую таблицу
#             for event in load_queue:
#                 self.stg.insert_event(conn, event)

#             # Обновляем курсор — максимальный id из загруженных событий
#             max_id = max(event.id for event in load_queue)
#             wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max_id
#             wf_setting_json = json2str(wf_setting.workflow_settings)
#             self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

#             self.log.info(f"Load finished on outbox id {max_id}")