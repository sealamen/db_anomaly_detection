import asyncio
from services.metrics_service import *
from services.detect_service import *

def detect() :
    key_fields = ["time", "host_cpu_util_pct", "active_sessions"]
    try :
        # targetDB에서 현재 상태 가져오기
        print("[START] Detecting metrics...")
        row = get_ser_metrics()
        if not row :
            print("[ERROR] Get Metrics Fail...")
            return
        # row 값 확인
        summary = {k: row.get(k) for k in key_fields if k in row}
        print(f"  → Summary: {summary}")

        # 이상치 여부 확인
        anomaly_res = detect_anomaly(row)
        row['anomaly_yn'] = anomaly_res
        print(f"  → Anomaly: {anomaly_res}")

        # LogDB에 값 넣기
        log_insert_res = insert_ser_perf_log(row)
        print(f"[DONE] {row['time']} | Log insert: {log_insert_res}\n")
    except Exception as e :
        print(f"[ERROR] detect failed: {e}")
# 주기적으로 detect 실행
async def detect_loop() :
    print(">>> Detect loop started (interval: 10s) <<<")
    while True :
        try :
            detect()
        except Exception as e :
            print(f"[ERROR] detect failed: {e}")
        await asyncio.sleep(10)



