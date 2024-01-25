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


class FctProductObject(BaseModel):
    id: int
    product_id: int
    order_id: int
    count: int
    price: float
    total_sum: float
    bonus_payment: float
    bonus_grant: float


class FctProductSource:
    def __init__(self, src_conn: PgConnect):
        self._db = src_conn

    def list_fct_product_sales_loader(self, threshold: int, limit: int) -> List[FctProductObject]:
        with self._db.client().cursor(row_factory=class_row(FctProductObject)) as cur:
            cur.execute(
                """
                    with fct as (select be.id,
                                        json_array_elements((be.event_value::json ->> 'product_payments')::json) ->>
                                        'product_id'                        as product_id,
                                        be.event_value::json ->> 'order_id' as order_id,
                                        json_array_elements((be.event_value::json ->> 'product_payments')::json) ->>
                                        'quantity'                          as count,
                                        json_array_elements((be.event_value::json ->> 'product_payments')::json) ->>
                                        'price'                             as price,
                                        json_array_elements((be.event_value::json ->> 'product_payments')::json) ->>
                                        'product_cost'                      as total_sum,
                                        json_array_elements((be.event_value::json ->> 'product_payments')::json) ->>
                                        'bonus_payment'                     as bonus_payment,
                                        json_array_elements((be.event_value::json ->> 'product_payments')::json) ->>
                                        'bonus_grant'                       as bonus_grant
                                 from stg.bonussystem_events be
                                 where event_type = 'bonus_transaction')
                    select fct.id,
                           dmp.id as product_id,
                           dmo.id as order_id,
                           fct.count,
                           fct.price,
                           fct.total_sum,
                           fct.bonus_payment,
                           fct.bonus_grant
                    from fct
                             inner join dds.dm_products dmp on dmp.product_id = fct.product_id
                             inner join dds.dm_orders dmo on dmo.order_key = fct.order_id
                      and fct.id > %(threshold)s                      
                    order by fct.id asc
                    limit %(limit)s
                """,
                {
                    "threshold": threshold,
                    "limit": limit
                },
            )
            objects = cur.fetchall()
            return objects
class FctProductTarget:
    def __init__(self, trg_conn: PgConnect):
        self._db = trg_conn

    def save_fct_product_sales_loader(self, conn: Connection, fct_product_sale: FctProductObject) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    insert into dds.fct_product_sales (product_id, order_id, count, price, total_sum, bonus_payment, bonus_grant)
                    values (%(product_id)s, %(order_id)s, %(count)s, %(price)s, %(total_sum)s, %(bonus_payment)s, %(bonus_grant)s)
                """,
                {
                    "product_id": fct_product_sale.product_id,
                    "order_id": fct_product_sale.order_id,
                    "count": fct_product_sale.count,
                    "price": fct_product_sale.price,
                    "total_sum": fct_product_sale.total_sum,
                    "bonus_payment": fct_product_sale.bonus_payment,
                    "bonus_grant": fct_product_sale.bonus_grant
                },
            )

class FctProductLoader:
    WF_KEY = "fct_product_sales_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "id"
    BATCH_LIMIT = 3000000

    def __init__(self, src_conn: PgConnect, trg_conn: PgConnect, log: Logger):
        self._src_conn = src_conn
        self._trg_conn = trg_conn
        self.src = FctProductSource(src_conn)
        self.trg = FctProductTarget(trg_conn)
        self.log = log
        self.settings_repository = StgEtlSettingsRepository()

    def load_fct_product_sale(self):
        with self._trg_conn.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if wf_setting is None:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})
            self.log.info(f"Loaded {wf_setting}")

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.src.list_fct_product_sales_loader(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} fct_product_sales to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return
            # Сохраняем объекты в базу dwh.
            for fct_product_sale in load_queue:
                last_loaded = max(last_loaded, fct_product_sale.id)
                self.trg.save_fct_product_sales_loader(conn, fct_product_sale)

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = last_loaded
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, self.WF_KEY, wf_setting_json)
