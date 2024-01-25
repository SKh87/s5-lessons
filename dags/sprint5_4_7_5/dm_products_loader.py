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


class DmProductObject(BaseModel):
    id: int
    restaurant_id: int
    product_id: str
    product_name: str
    product_price: float
    active_from: datetime
    active_to: datetime


class DmProductSource:
    def __init__(self, src_conn: PgConnect):
        self._db = src_conn

    def list_dm_product(self, threshold: int, limit: int) -> List[DmProductObject]:
        with self._db.client().cursor(row_factory=class_row(DmProductObject)) as cur:
            cur.execute(
                """
                    select
                        osr.id,
                        dmr.id as restaurant_id,
                        json_array_elements((osr.object_value::json->>'menu')::json)->'_id' as product_id,
                        json_array_elements((osr.object_value::json->>'menu')::json)->'name' as product_name,
                        json_array_elements((osr.object_value::json->>'menu')::json)->'price' as product_price,
                        osr.update_ts as active_from,
                        '2099-12-31 00:00:00.000'::timestamp as active_to
                    from stg.ordersystem_restaurants osr
                    inner join dds.dm_restaurants  dmr on dmr.restaurant_id = osr.object_value::json->>'_id'
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
class DmProductTarget:
    def __init__(self, trg_conn: PgConnect):
        self._db = trg_conn

    def save_dm_product(self, conn: Connection, dm_product: DmProductObject) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    insert into dds.dm_products (restaurant_id, product_id, product_name, product_price, active_from, active_to)
                    values (%(restaurant_id)s, %(product_id)s, %(product_name)s, %(product_price)s, %(active_from)s, %(active_to)s)
                """,
                {
                    "restaurant_id": dm_product.restaurant_id,
                    "product_id": dm_product.product_id,
                    "product_name": dm_product.product_name,
                    "product_price": dm_product.product_price,
                    "active_from": dm_product.active_from,
                    "active_to": dm_product.active_to
                },
            )

class DmProductLoader:
    WF_KEY = "dm_product_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "id"
    BATCH_LIMIT = 3000000

    def __init__(self, src_conn: PgConnect, trg_conn: PgConnect, log: Logger):
        self._src_conn = src_conn
        self._trg_conn = trg_conn
        self.src = DmProductSource(src_conn)
        self.trg = DmProductTarget(trg_conn)
        self.log = log
        self.settings_repository = StgEtlSettingsRepository()

    def load_dm_product(self):
        with self._trg_conn.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if wf_setting is None:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})
            self.log.info(f"Loaded {wf_setting}")

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.src.list_dm_product(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} dm_product to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return
            # Сохраняем объекты в базу dwh.
            for dm_product in load_queue:
                last_loaded = max(last_loaded, dm_product.id)
                self.trg.save_dm_product(conn, dm_product)

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = last_loaded
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, self.WF_KEY, wf_setting_json)
