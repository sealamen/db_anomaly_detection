import pandas as pd
import numpy as np
import joblib
from tensorflow.keras.models import load_model

from mappers.detect_mapper import *

def detect_anomaly(row_dict):

    model_path = "C:\\OCI\\repository\\db_anomaly_detection_AI\\models\\"
    df = pd.DataFrame([row_dict])

    # 1. 전처리
    df.replace("null", 0, inplace=True)

    # 숫자형 컬럼만 선택, time 제거
    df_numeric = df.select_dtypes(include=['float64', 'int64']).drop(columns=['time'], errors='ignore') # 숫자형 컬럼만 선택 (time 제거)


    # 2. One-Class SVM 예측
    svm_bundle = joblib.load(model_path + "one_class_svm.pkl")
    oc_svm = svm_bundle["svm_model"]
    svm_scaler = svm_bundle["scaler"]
    svm_features = svm_bundle["feature_columns"]

    # 누락된 컬럼 채우기
    for col in svm_features:
        if col not in df_numeric.columns:
            df_numeric[col] = 0  # 또는 np.nan

    # 컬럼 순서 맞추기
    df_svm_input = df_numeric[svm_features]
    svm_scaled = svm_scaler.transform(df_svm_input)
    svm_preds = oc_svm.predict(svm_scaled)
    df_numeric["SVM_Pred"] = np.where(svm_preds == -1, 1, 0)

    # 3. Isolation Forest 예측
    if_bundle = joblib.load(model_path + "isolation_forest.pkl")
    iso_forest = if_bundle["iso_model"]  # 저장 시 key 이름
    if_features = if_bundle["feature_columns"]

    # Isolation Forest는 컬럼 순서만 맞춰주면 됨 (학습 시 컬럼 순서 사용)
    if_preds = iso_forest.predict(df_numeric[if_features])  # feature만 사용
    df_numeric["IF_Pred"] = np.where(if_preds == -1, 1, 0)

    # 4. Autoencoder 예측
    autoencoder = load_model(model_path + "autoencoder.h5", compile=False)
    ae_bundle = joblib.load(model_path + "autoencoder_scaler_columns.pkl")
    ae_scaler = ae_bundle["scaler"]
    ae_features = ae_bundle["feature_columns"]

    # 누락된 컬럼 채우기
    for col in ae_features:
        if col not in df_numeric.columns:
            df_numeric[col] = 0

    df_ae_input = df_numeric[ae_features]
    ae_scaled = ae_scaler.transform(df_ae_input)
    recon = autoencoder.predict(ae_scaled, verbose=0)
    mse = np.mean(np.square(ae_scaled - recon), axis=1)

    # 임계값: 평균 + 3*표준편차
    recon_thresh = mse.mean() + 3 * mse.std()
    df_numeric["AE_Pred"] = (mse > recon_thresh).astype(int)

    # 5. Final Alert
    df_numeric['Final_Alert'] = df_numeric.apply(final_alert, axis=1)
    anomaly_yn = 'Y' if df_numeric['Final_Alert'].iloc[0] == 1 else 'N'

    # 6. 결과 확인
    print("✅ 최종 결과 샘플:")
    print(df_numeric[['IF_Pred', 'SVM_Pred', 'AE_Pred', 'Final_Alert']].head())
    print(anomaly_yn)

    return anomaly_yn

def final_alert(row):
    votes = row[['IF_Pred', 'SVM_Pred', 'AE_Pred']].sum()
    return 1 if votes >= 2 else 0


# row = {
#   "time": "2025-09-19T11:24:38.095000",
#   "host_cpu_util_pct": 8.6068638856573,
#   "host_cpu_usage_per_sec": 68.8434695912263,
#   "db_cpu_time_ratio": 0,
#   "db_cpu_usage_per_sec": 0,
#   "cpu_usage_per_txn": 0,
#   "bg_cpu_usage_per_sec": 0,
#   "buffer_cache_hit_ratio": 100,
#   "shared_pool_free_pct": 95.2063103561985,
#   "library_cache_hit_ratio": 100,
#   "sessions_total": 24,
#   "active_sessions": "null",
#   "logons_per_sec": 0,
#   "process_count": "null",
#   "physical_reads_per_sec": 0,
#   "physical_writes_per_sec": 0,
#   "redo_writes_per_sec": 0,
#   "io_requests_per_sec": 2.12695247590562,
#   "io_throughput_mb_sec": 0.0332336324360253,
#   "avg_read_latency_ms": "null",
#   "avg_write_latency_ms": "null",
#   "db_time_ms": 0,
#   "cpu_time_ms": "null",
#   "user_io_wait_ms": "null",
#   "system_io_wait_ms": "null",
#   "log_file_sync_wait_ms": "null",
#   "concurrency_wait_ms": "null",
#   "txn_per_sec": "null",
#   "user_calls_per_sec": 0,
#   "executions_per_sec": 0.398803589232303,
#   "parse_count_per_sec": "null",
#   "hard_parse_ratio_pct": "null",
#   "sga_free_mb": 31,
#   "pga_used_mb": 35
# }
#
#
# detect_anomaly(row)