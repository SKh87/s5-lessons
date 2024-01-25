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


class DmRestaurantObject(BaseModel):
    id: int
    restaurant_id: str
    restaurant_name: str
    active_from: datetime
    active_to: datetime


class DmRestaurantSource:
    def __init__(self, src_conn: PgConnect):
        self._db = src_conn

    def list_dm_restaurant(self, threshold: int, limit: int) -> List[DmRestaurantObject]:
        with self._db.client().cursor(row_factory=class_row(DmRestaurantObject)) as cur:
            cur.execute(
                """
                    select
                        osr.id,
                        osr.object_id as restaurant_id,
                        osr.object_value::json->'name' as restaurant_name,
                        osr.update_ts as activate_from,
                        '2099-12-31 00:00:00.000'::timestamp as active_to
                    from stg.ordersystem_restaurants osr
                    where osr.id > %(threshold)s
                    order by osr.id asc
                    limit %(limit)s
                """,
                {
                    "threshold": threshold,
                    "limit": limit
                },
            )
            objects = cur.fetchall()
            return objects
class DmRestaurantTarget:
    def __init__(self, trg_conn: PgConnect):
        self._db = trg_conn

    def save_dm_restaurant(self, conn: Connection, dm_restaurant: DmRestaurantObject) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    update dds.dm_restaurants
                    set active_to = %(active_from)s
                    where restaurant_id=%(restaurant_id)s
                    and active_from='2099-12-31 00:00:00.000'::timestamp
                """,
                {
                    "active_from": dm_restaurant.active_from,
                    "restaurant_id": dm_restaurant.restaurant_id
                },
            )
            cur.execute(
                """
                    insert into dds.dm_restaurants (restaurant_id, restaurant_name, active_from, active_to)
                    values (%(restaurant_id)s, %(restaurant_name)s, %(active_from)s, %(active_to)s)
                """,
                {
                    "restaurant_id": dm_restaurant.restaurant_id,
                    "restaurant_name": dm_restaurant.restaurant_name,
                    "active_from": dm_restaurant.active_from,
                    "active_to": dm_restaurant.active_to
                },
            )

class DmRestaurantLoader:
    WF_KEY = "dm_restaurant_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "id"
    BATCH_LIMIT = 100

    def __init__(self, src_conn: PgConnect, trg_conn: PgConnect, log: Logger):
        self._src_conn = src_conn
        self._trg_conn = trg_conn
        self.src = DmRestaurantSource(src_conn)
        self.trg = DmRestaurantTarget(trg_conn)
        self.log = log
        self.settings_repository = StgEtlSettingsRepository()

    def load_dm_restaurant(self):
        with self._trg_conn.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if wf_setting is None:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})
            self.log.info(f"Loaded {wf_setting}")

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.src.list_dm_restaurant(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} dm_restaurant to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return
            # Сохраняем объекты в базу dwh.
            for dm_restaurant in load_queue:
                last_loaded = max(last_loaded, dm_restaurant.id)
                self.trg.save_dm_restaurant(conn, dm_restaurant)

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = last_loaded
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, self.WF_KEY, wf_setting_json)
