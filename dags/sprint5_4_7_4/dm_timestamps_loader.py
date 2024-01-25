from datetime import datetime, date, time
from logging import Logger
from typing import List

# from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel

from examples.stg import StgEtlSettingsRepository, EtlSetting
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection


class DmTimestampObject(BaseModel):
    id: int
    ts: datetime
    year: int
    month: int
    day: int
    time: time
    date: date


class DmTimestampSource:
    def __init__(self, src_conn: PgConnect):
        self._db = src_conn

    def list_dm_timestamp(self, threshold: int, limit: int) -> List[DmTimestampObject]:
        with self._db.client().cursor(row_factory=class_row(DmTimestampObject)) as cur:
            cur.execute(
                """
                    with t as(
                        select
                            oso.id,
                            (oso.object_value::json->>'date')::timestamp as ts
                        from stg.ordersystem_orders oso
                        where oso.id > %(threshold)s                         
                        and oso.object_value::json->>'final_status' in ('CANCELLED', 'CLOSED') 
                        order by oso.id asc
                        limit %(limit)s
                    )
                    select
                        t.id,
                        t.ts,
                        extract(year from t.ts) as year,
                        extract(month from t.ts) as month,
                        extract(day from t.ts) as day,
                        t.ts::time as time,
                        t.ts::date as date
                    from t
                """,
                {
                    "threshold": threshold,
                    "limit": limit
                },
            )
            objects = cur.fetchall()
            return objects
class DmTimestampTarget:
    def __init__(self, trg_conn: PgConnect):
        self._db = trg_conn

    def save_dm_timestamp(self, conn: Connection, dm_timestamp: DmTimestampObject) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    insert into dds.dm_timestamps (ts, year, month, day, time, date)
                    values (%(ts)s, %(year)s, %(month)s, %(day)s, %(time)s, %(date)s)
                """,
                {
                    "ts": dm_timestamp.ts,
                    "year": dm_timestamp.year,
                    "month": dm_timestamp.month,
                    "day": dm_timestamp.day,
                    "time": dm_timestamp.time,
                    "date": dm_timestamp.date
                },
            )

class DmTimestampLoader:
    WF_KEY = "dm_timestamp_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "id"
    BATCH_LIMIT = 100

    def __init__(self, src_conn: PgConnect, trg_conn: PgConnect, log: Logger):
        self._src_conn = src_conn
        self._trg_conn = trg_conn
        self.src = DmTimestampSource(src_conn)
        self.trg = DmTimestampTarget(trg_conn)
        self.log = log
        self.settings_repository = StgEtlSettingsRepository()

    def load_dm_timestamp(self):
        with self._trg_conn.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if wf_setting is None:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})
            self.log.info(f"Loaded {wf_setting}")

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.src.list_dm_timestamp(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} dm_timestamp to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return
            # Сохраняем объекты в базу dwh.
            for dm_timestamp in load_queue:
                last_loaded = max(last_loaded, dm_timestamp.id)
                self.trg.save_dm_timestamp(conn, dm_timestamp)

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = last_loaded
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, self.WF_KEY, wf_setting_json)
