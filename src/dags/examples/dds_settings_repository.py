from typing import Dict, Optional

from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class EtlSetting(BaseModel):
    id: int
    workflow_key: str
    workflow_settings: Dict


class DdsEtlSettingsRepository:
    def get_setting(self, conn: Connection, etl_key: str) -> Optional[EtlSetting]:
        with conn.cursor(row_factory=class_row(EtlSetting)) as cur:
            cur.execute(
                """
                    SELECT
                        id,
                        workflow_key,
                        workflow_settings
                    FROM dds.srv_wf_settings
                    WHERE workflow_key = %(etl_key)s;
                """,
                {"etl_key": etl_key},
            )
            obj = cur.fetchone()

        return obj

    def save_setting(self, conn: Connection, sett: EtlSetting) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.srv_wf_settings(workflow_key, workflow_settings)
                    VALUES (%(wf_key)s, %(wf_setting)s)
                    ON CONFLICT (workflow_key) DO UPDATE
                    SET workflow_settings = EXCLUDED.workflow_settings;
                """,
                {
                    "wf_key": sett.workflow_key,
                    "wf_setting": json2str(sett.workflow_settings)
                },
            )
