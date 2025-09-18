from fastapi import APIRouter
from services.metrics_service import *

router = APIRouter(prefix="/metrics", tags=["metrics"])

@router.get("/sessions")
def sessions():
    return get_perf_metrics()
