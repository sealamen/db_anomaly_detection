from fastapi import FastAPI
from controllers import metrics_controller

app = FastAPI()
app.include_router(metrics_controller.router)

@app.get("/")
def root():
    return {"message": "Hello World"}
