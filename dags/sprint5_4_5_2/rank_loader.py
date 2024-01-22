from pathlib import Path
def rank_loader(src_pg_conn, trg_pg_conn, limit: int):
    offset: int = 0
    with open(str(Path(__file__).parent) + "src_ranks.sql", "r") as file:
        select_ranks_query = file.read()
    with open(str(Path(__file__).parent) + "trg_ranks.sql", "r") as file:
        insert_ranks_query = file.read()

    with src_pg_conn.cursor() as src_cursor:
        with trg_pg_conn.cursor() as trg_cursor:
            while True:
                src_cursor.execute(select_ranks_query, {"offset": offset, "limit": limit})
                column_names = [desc[0] for desc in src_cursor.description]
                print(column_names)
                rows = src_cursor.fetchall()
                if len(rows) == 0:
                    break
                for row in rows:
                    params = dict(zip(column_names, row))
                    trg_cursor.execute(insert_ranks_query, params)
                offset = offset + limit


if __name__ == "__main__":
    import psycopg2
    from dotenv import dotenv_values

    config = dotenv_values("../../.env")
    print(config)

    src_pg_conn = psycopg2.connect(
        database=config.get("PG_YANDEX_DB"),
        user=config.get("PG_YANDEX_USER"),
        password=config.get("PG_YANDEX_PASSWORD"),
        host=config.get("PG_YANDEX_HOST"),
        port=config.get("PG_YANDEX_PORT")
    )
    trg_pg_conn = psycopg2.connect(
        database=config.get("PG_LOCAL_DB"),
        user=config.get("PG_LOCAL_USER"),
        password=config.get("PG_LOCAL_PASSWORD"),
        host=config.get("PG_LOCAL_HOST"),
        port=config.get("PG_LOCAL_PORT")
    )
    rank_loader(src_pg_conn, trg_pg_conn, 1)
