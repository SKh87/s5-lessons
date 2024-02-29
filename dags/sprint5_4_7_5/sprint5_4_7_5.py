import logging

import pendulum
from airflow.decorators import dag, task
from lib import ConnectionBuilder


from sprint5_4_7_5.dm_products_loader import DmProductLoader

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'stg'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def sprint5_4_7_5():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Создаем подключение к базе подсистемы бонусов.
    origin_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")


    @task(task_id="product_load")
    def load_dm_product():
        rest_loader = DmProductLoader(origin_pg_connect, dwh_pg_connect, log)
        rest_loader.load_dm_product()

    dm_products_task = load_dm_product()

    # Объявляем третий таск, который загружает данные outbox
    dm_products_task

    
stg_bonus_system_ranks_dag = sprint5_4_7_5()
