from fastapi import APIRouter
from services.metrics_service import *

router = APIRouter(prefix="/metrics", tags=["metrics"])

@router.get("/getMetrics")
def get_metrics():
    return get_ser_metrics()
