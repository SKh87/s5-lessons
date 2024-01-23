from logging import Logger
from typing import List

# from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel

from examples.stg import StgEtlSettingsRepository, EtlSetting
from lib import PgConnect
from lib.dict_util import json2str


class UsersObject(BaseModel):
    id: int
    order_user_id: str


class UserSource:
    def __init__(self, src_conn: PgConnect):
        self._db = src_conn

    def list_users(self, threshold_user_id: int, limit: int) -> List[UsersObject]:
        with self._db.client().cursor(row_factory=class_row(UsersObject)) as cur:
            cur.execute(
                """
                    select id, order_user_id
                    from users
                    where id > %(threshold_user_id)s
                    order by id asc
                    limit %(limit)s
                """,
                {
                    "threshold_user_id": threshold_user_id,
                    "limit": limit
                },
            )
            objects = cur.fetchall()
            return objects


class UserTarget:
    def __init__(self, trg_conn: PgConnect):
        self._db = trg_conn

    def save_user(self, user: UsersObject) -> None:
        with self._db.client().cursor() as cur:
            cur.execute(
                """
                    insert into stg.bonussystem_users(id, order_user_id) 
                    values (%(id)s, %(order_user_id)s)
                    on conflict (id) do update 
                        set order_user_id = excluded.order_user_id
                """,
                {
                    "id": user.id,
                    "order_user_id": user.order_user_id
                },
            )


class UsersLoader:
    WF_KEY = "users_source_to_stg_workflow"
    LAST_LOADED_ID_KEY = "id"
    BATCH_LIMIT = 10

    def __init__(self, src_conn: PgConnect, trg_conn: PgConnect, log: Logger):
        self._src_conn = src_conn
        self._trg_conn = trg_conn
        self.src = UserSource(src_conn)
        self.trg = UserTarget(trg_conn)
        self.log = log
        self.settings_repository = StgEtlSettingsRepository()

    def load_users(self):
        with self._trg_conn.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if wf_setting is None:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})
            self.log.info(f"Loaded {wf_setting}")

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.src.list_users(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} ranks to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return
            # Сохраняем объекты в базу dwh.
            for user in load_queue:
                last_loaded = max(last_loaded, user.id)
                self.log.info(user)
                self.trg.save_user(user)

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = last_loaded
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, self.WF_KEY, wf_setting_json)

# if __name__ == "__main__":
#     log = Logger(name="aaa")
#     from dotenv import dotenv_values
#     config = dotenv_values("../../.env")
#     print(config)
#
#     src_conn = PgConnect(
#         db_name=config.get("PG_YANDEX_DB"),
#         user=config.get("PG_YANDEX_USER"),
#         pw=config.get("PG_YANDEX_PASSWORD"),
#         host=config.get("PG_YANDEX_HOST"),
#         port=config.get("PG_YANDEX_PORT")
#     )
#     trg_conn = PgConnect(
#         db_name=config.get("PG_LOCAL_DB"),
#         user=config.get("PG_LOCAL_USER"),
#         pw=config.get("PG_LOCAL_PASSWORD"),
#         host=config.get("PG_LOCAL_HOST"),
#         port=config.get("PG_LOCAL_PORT")
#     )
#     UsersLoader(src_conn, trg_conn, log).load_users()
