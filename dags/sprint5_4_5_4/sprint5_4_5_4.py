import logging

import pendulum
from airflow.decorators import dag, task
from lib import ConnectionBuilder

from examples.stg.bonus_system_ranks_dag.ranks_loader import RankLoader
from sprint5_4_5_3.users_loader import UsersLoader
from sprint5_4_5_4.events_loader import EventsLoader

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'stg'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def sprint5_4_5_4():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Создаем подключение к базе подсистемы бонусов.
    origin_pg_connect = ConnectionBuilder.pg_conn("PG_ORIGIN_BONUS_SYSTEM_CONNECTION")

    # Объявляем таск, который загружает данные.
    @task(task_id="ranks_load")
    def load_ranks():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = RankLoader(origin_pg_connect, dwh_pg_connect, log)
        rest_loader.load_ranks()  # Вызываем функцию, которая перельет данные.

    # Инициализируем объявленные таски.
    ranks_dict = load_ranks()

    # Объявляем второй таск, который загружает данные по users
    @task(task_id="users_load")
    def load_users():
        rest_loader = UsersLoader(origin_pg_connect, dwh_pg_connect, log)
        rest_loader.load_users()

    users_task = load_users()

    # Объявляем третий таск, который загружает данные outbox
    @task(task_id="events_load")
    def load_events():
        rest_loader = EventsLoader(origin_pg_connect, dwh_pg_connect, log)
        rest_loader.load_events()

    events_task = load_events()

    # Далее задаем последовательность выполнения тасков.
    # Т.к. таск один, просто обозначим его здесь.
    ranks_dict  # type: ignore
    users_task
    events_task


stg_bonus_system_ranks_dag = sprint5_4_5_4()
