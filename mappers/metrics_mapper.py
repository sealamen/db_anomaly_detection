from db_conn import get_pool

def get_mapper_metrics():
    pool = get_pool("target_db")   # 현재는 db1만 사용
    with pool.acquire() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM db_perf_view")
            cols = [d[0].lower() for d in cur.description]
            row = cur.fetchone()
            return dict(zip(cols, row)) if row else None
