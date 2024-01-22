from airflow.decorators import dag, task
from sqlalchemy_utils.types.enriched_datetime.pendulum_date import pendulum
from airflow.providers.postgres.hooks.postgres import PostgresHook

import sys
from pathlib import Path

# Append the new path
sys.path.append(str(Path(__file__).parent))
from rank_loader import rank_loader
@dag(
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['sprint5'],
    is_paused_upon_creation=False
)
def sprint5_4_5_2():

    @task
    def rank_loader_task():
        src_pg_hook = PostgresHook("PG_ORIGIN_BONUS_SYSTEM_CONNECTION")
        trg_pg_hook = PostgresHook("PG_WAREHOUSE_CONNECTION")

        src_pg_conn = src_pg_hook.get_conn()
        trg_pg_conn = trg_pg_hook.get_conn()

        rank_loader(src_pg_conn, trg_pg_conn, 1)

    rank_loader_task()

sprint5_4_5_2()
