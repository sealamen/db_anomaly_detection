import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI
from controllers import metrics_controller
from ScheduleDetector import *

@asynccontextmanager
async def lifespan(app: FastAPI):
    # startup 시 실행
    task = asyncio.create_task(detect_loop())
    yield
    # shutdown 시 실행
    task.cancel()
app = FastAPI(lifespan=lifespan)
app.include_router(metrics_controller.router)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
