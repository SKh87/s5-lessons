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


class DmUsersObject(BaseModel):
    id: int
    user_id: str
    user_name: str
    user_login:str

class DmUsersSource:
    def __init__(self, src_conn: PgConnect):
        self._db = src_conn

    def list_dm_user(self, threshold: int, limit: int) -> List[DmUsersObject]:
        with self._db.client().cursor(row_factory=class_row(DmUsersObject)) as cur:
            cur.execute(
                """
                    select bu.id                             as id,
                           bu.order_user_id                  as user_id,
                           ou.object_value::json ->> 'name'  as user_name,
                           ou.object_value::json ->> 'login' as user_login
                    from stg.bonussystem_users bu
                             inner join stg.ordersystem_users ou on ou.object_id = bu.order_user_id
                    where bu.id > %(threshold)s
                    order by bu.id asc
                    limit %(limit)s
                """,
                {
                    "threshold": threshold,
                    "limit": limit
                },
            )
            objects = cur.fetchall()
            return objects
class DmUsersTarget:
    def __init__(self, trg_conn: PgConnect):
        self._db = trg_conn

    def save_dm_user(self, conn: Connection, user: DmUsersObject) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    insert into dds.dm_users (user_id, user_name, user_login)
                    values (%(user_id)s, %(user_name)s, %(user_login)s )
                    /* ToDo добавить уникальность на user_id и добавить on conflict */
                """,
                {
                    "user_id": user.user_id,
                    "user_name": user.user_name,
                    "user_login": user.user_login
                },
            )

class DmUsersLoader:
    WF_KEY = "user_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "id"
    BATCH_LIMIT = 3000

    def __init__(self, src_conn: PgConnect, trg_conn: PgConnect, log: Logger):
        self._src_conn = src_conn
        self._trg_conn = trg_conn
        self.src = DmUsersSource(src_conn)
        self.trg = DmUsersTarget(trg_conn)
        self.log = log
        self.settings_repository = StgEtlSettingsRepository()

    def load_dm_users(self):
        with self._trg_conn.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if wf_setting is None:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})
            self.log.info(f"Loaded {wf_setting}")

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.src.list_dm_user(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} user to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return
            # Сохраняем объекты в базу dwh.
            for dm_user in load_queue:
                last_loaded = max(last_loaded, dm_user.id)
                self.trg.save_dm_user(conn, dm_user)

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = last_loaded
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, self.WF_KEY, wf_setting_json)
