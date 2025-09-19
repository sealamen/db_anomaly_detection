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


# db_perf_log_insert
def insert_ser_perf_log(row : dict) :
    return insert_perf_log(row)