from datetime import datetime
from logging import Logger
from typing import List

# from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel

from examples.stg import StgEtlSettingsRepository, EtlSetting
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection


class EventsObject(BaseModel):
    id: int
    event_ts: datetime
    event_type: str
    event_value: str


class EventSource:
    def __init__(self, src_conn: PgConnect):
        self._db = src_conn

    def list_events(self, threshold: int, limit: int) -> List[EventsObject]:
        with self._db.client().cursor(row_factory=class_row(EventsObject)) as cur:
            cur.execute(
                """
                    select 
                        id,
                        event_ts,
                        event_type,
                        event_value
                    from public.outbox
                    where id > %(threshold)s
                    order by id asc
                    limit %(limit)s
                """,
                {
                    "threshold": threshold,
                    "limit": limit
                },
            )
            objects = cur.fetchall()
            return objects


class EventTarget:
    def __init__(self, trg_conn: PgConnect):
        self._db = trg_conn

    def save_event(self, conn: Connection, event: EventsObject) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    insert into stg.bonussystem_events(id, event_ts, event_type, event_value) 
                    values (
                        %(id)s, %(event_ts)s, %(event_type)s, %(event_value)s )
                    on conflict (id) do update 
                        set 
                            event_ts = excluded.event_ts,
                            event_type = excluded.event_type,
                            event_value = excluded.event_value
                """,
                {
                    "id": event.id,
                    "event_ts": event.event_ts,
                    "event_type": event.event_type,
                    "event_value": event.event_value
                },
            )


class EventsLoader:
    WF_KEY = "event_source_to_stg_workflow"
    LAST_LOADED_ID_KEY = "id"
    BATCH_LIMIT = 3000000

    def __init__(self, src_conn: PgConnect, trg_conn: PgConnect, log: Logger):
        self._src_conn = src_conn
        self._trg_conn = trg_conn
        self.src = EventSource(src_conn)
        self.trg = EventTarget(trg_conn)
        self.log = log
        self.settings_repository = StgEtlSettingsRepository()

    def load_events(self):
        with self._trg_conn.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if wf_setting is None:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})
            self.log.info(f"Loaded {wf_setting}")

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.src.list_events(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} event to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return
            # Сохраняем объекты в базу dwh.
            for event in load_queue:
                last_loaded = max(last_loaded, event.id)
                self.trg.save_event(conn, event)

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = last_loaded
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, self.WF_KEY, wf_setting_json)
