import logging

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from app.routes.web import router as web_router

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

app = FastAPI(title="Queue Analytics Service")

app.mount("/static", StaticFiles(directory="app/static"), name="static")
app.include_router(web_router)