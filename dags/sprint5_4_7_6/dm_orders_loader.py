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


class DmOrderObject(BaseModel):
    id: int
    order_key: str
    order_status: str
    restaurant_id: int
    timestamp_id: int
    user_id: int


class DmOrderSource:
    def __init__(self, src_conn: PgConnect):
        self._db = src_conn

    def list_dm_order(self, threshold: int, limit: int) -> List[DmOrderObject]:
        with self._db.client().cursor(row_factory=class_row(DmOrderObject)) as cur:
            cur.execute(
                """
                    select oso.id,
                           oso.object_id                        as order_key,
                           object_value::json -> 'final_status' as order_status,
                           dmr.id                               as restaurant_id,
                           dmt.id                               as timestamp_id,
                           dmu.id                               as user_id
                    from stg.ordersystem_orders oso
                             inner join dds.dm_restaurants dmr on dmr.restaurant_id = (object_value::json ->> 'restaurant')::json ->> 'id'
                             inner join dds.dm_timestamps dmt on dmt.ts = (object_value::json ->> 'date')::timestamp
                             inner join dds.dm_users dmu on dmu.user_id = (object_value::json ->> 'user')::json ->> 'id'
                    where 1 = 1
                      and oso.id > %(threshold)s
                      and oso.object_value::json ->> 'final_status' in ('CANCELLED', 'CLOSED')
                    order by oso.id asc
                    limit %(limit)s
                """,
                {
                    "threshold": threshold,
                    "limit": limit
                },
            )
            objects = cur.fetchall()
            return objects
class DmOrderTarget:
    def __init__(self, trg_conn: PgConnect):
        self._db = trg_conn

    def save_dm_order(self, conn: Connection, dm_order: DmOrderObject) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    insert into dds.dm_orders (order_key,order_status,restaurant_id,timestamp_id,user_id)
                    values (%(order_key)s,%(order_status)s,%(restaurant_id)s,%(timestamp_id)s,%(user_id)s)
                """,
                {
                    "order_key": dm_order.order_key,
                    "order_status": dm_order.order_status,
                    "restaurant_id": dm_order.restaurant_id,
                    "timestamp_id": dm_order.timestamp_id,
                    "user_id": dm_order.user_id
                },
            )

class DmOrderLoader:
    WF_KEY = "dm_order_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "id"
    BATCH_LIMIT = 150

    def __init__(self, src_conn: PgConnect, trg_conn: PgConnect, log: Logger):
        self._src_conn = src_conn
        self._trg_conn = trg_conn
        self.src = DmOrderSource(src_conn)
        self.trg = DmOrderTarget(trg_conn)
        self.log = log
        self.settings_repository = StgEtlSettingsRepository()

    def load_dm_order(self):
        with self._trg_conn.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if wf_setting is None:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})
            self.log.info(f"Loaded {wf_setting}")

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.src.list_dm_order(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} dm_order to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return
            # Сохраняем объекты в базу dwh.
            for dm_order in load_queue:
                last_loaded = max(last_loaded, dm_order.id)
                self.trg.save_dm_order(conn, dm_order)

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = last_loaded
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, self.WF_KEY, wf_setting_json)
