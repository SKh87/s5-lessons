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


class DmSsettlementReportObject(BaseModel):
    restaurant_id: str
    restaurant_name: str
    settlement_date: date
    orders_count: int
    orders_total_sum: float
    orders_bonus_payment_sum: float
    orders_bonus_granted_sum: float
    order_processing_fee: float
    restaurant_reward_sum: float


class DmSsettlementReportSource:
    def __init__(self, src_conn: PgConnect):
        self._db = src_conn

    def list_dm_settlement_report(self, threshold: int, limit: int) -> List[DmSsettlementReportObject]:
        with self._db.client().cursor(row_factory=class_row(DmSsettlementReportObject)) as cur:
            cur.execute(
                """
                    select dr.restaurant_id,
                           dr.restaurant_name,
                           dt.date                                                               as settlement_date,
                           count(distinct o.id)                                                     as orders_count,
                           sum(fct.total_sum)                                                    as orders_total_sum,
                           sum(fct.bonus_payment)                                                as orders_bonus_payment_sum,
                           sum(fct.bonus_grant)                                                  as orders_bonus_granted_sum,
                           sum(fct.total_sum) * 0.25                                            as order_processing_fee,
                           sum(fct.total_sum) * 0.75 - sum(fct.bonus_payment) as restaurant_reward_sum
                    from dds.fct_product_sales fct
                             inner join dds.dm_orders o
                                        on o.id = fct.order_id
                             inner join dds.dm_timestamps dt
                                        on dt.id = o.timestamp_id
                             inner join dds.dm_users du
                                        on du.id = o.user_id
                             inner join dds.dm_products dp
                                        on dp.id = fct.product_id
                             inner join dds.dm_restaurants dr
                                        on dr.id = dp.restaurant_id
                                            and dr.id = o.restaurant_id
                    where o.order_status = 'CLOSED'
                        and dt.date> %(threshold)s
                    group by dr.restaurant_id, dr.restaurant_name, dt.date                    
                    order by dt.date
                    limit %(limit)s
                """,
                {
                    "threshold": threshold,
                    "limit": limit
                },
            )
            objects = cur.fetchall()
            return objects
class DmSsettlementReportTarget:
    def __init__(self, trg_conn: PgConnect):
        self._db = trg_conn

    def save_dm_settlement_report(self, conn: Connection, dm_settlement_report: DmSsettlementReportObject) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    insert into cdm.dm_settlement_report (restaurant_id, restaurant_name, settlement_date, orders_count, orders_total_sum, orders_bonus_payment_sum, orders_bonus_granted_sum, order_processing_fee, restaurant_reward_sum)
                    values ($(restaurant_id)s, $(restaurant_name)s, $(settlement_date)s, $(orders_count)s, $(orders_total_sum)s, $(orders_bonus_payment_sum)s, $(orders_bonus_granted_sum)s, $(order_processing_fee)s, $(restaurant_reward_sum)s)
                """,
                {
                    "restaurant_id": dm_settlement_report.restaurant_id,
                    "restaurant_name": dm_settlement_report.restaurant_name,
                    "settlement_date": dm_settlement_report.settlement_date,
                    "orders_count": dm_settlement_report.orders_count,
                    "orders_total_sum": dm_settlement_report.orders_total_sum,
                    "orders_bonus_payment_sum": dm_settlement_report.orders_bonus_payment_sum,
                    "orders_bonus_granted_sum": dm_settlement_report.orders_bonus_granted_sum,
                    "order_processing_fee": dm_settlement_report.order_processing_fee,
                    "restaurant_reward_sum": dm_settlement_report.restaurant_reward_sum,
                },
            )

class DmSsettlementReportLoader:
    WF_KEY = "dm_settlement_report_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "settlement_date"
    BATCH_LIMIT = 3000000

    def __init__(self, src_conn: PgConnect, trg_conn: PgConnect, log: Logger):
        self._src_conn = src_conn
        self._trg_conn = trg_conn
        self.src = DmSsettlementReportSource(src_conn)
        self.trg = DmSsettlementReportTarget(trg_conn)
        self.log = log
        self.settings_repository = StgEtlSettingsRepository()

    def load_dm_settlement_report(self):
        with self._trg_conn.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if wf_setting is None:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: datetime(2022, 1, 1).isoformat()})
            self.log.info(f"Loaded {wf_setting}")

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            last_loaded = datetime.fromisoformat(last_loaded)
            load_queue = self.src.list_dm_settlement_report(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} dm_settlement_report to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return
            # Сохраняем объекты в базу dwh.
            for dm_settlement_report in load_queue:
                last_loaded = max(last_loaded, dm_settlement_report.settlement_date)
                self.trg.save_dm_settlement_report(conn, dm_settlement_report)

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = last_loaded
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, self.WF_KEY, wf_setting_json)
