from db_conn import get_pool

def get_mapper_metrics():
    pool = get_pool("target_db")   # 현재는 db1만 사용
    with pool.acquire() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM db_perf_view")
            cols = [d[0].lower() for d in cur.description]  # 컬럼 이름 가져오기
            rows = cur.fetchall()
            return [dict(zip(cols, r)) for r in rows]       # [{"col1": val1, ...}, ...]
