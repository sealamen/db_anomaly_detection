from db_conn import get_pool

# db_perf_log_insert
def insert_perf_log(row : dict) :
    if not row :
        raise ValueError("row is empty")

    pool = get_pool("log_db")
    with pool.acquire() as conn:
        with conn.cursor() as cur:

            columns = ", ".join(row.keys())
            values = ", ".join([f":{k}" for k in row.keys()])

            sql = f"INSERT INTO db_perf_log ({columns}) VALUES ({values})"
            cur.execute(sql, row)
        conn.commit()
    return True