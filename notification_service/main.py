from contextlib import asynccontextmanager

from fastapi import FastAPI

from database import Base, engine
import models
from routers import notifications


@asynccontextmanager
async def lifespan(app: FastAPI):

    Base.metadata.create_all(bind=engine)

    yield


app = FastAPI(
    title="Notification Service",
    lifespan=lifespan
)

app.include_router(notifications.router)


@app.get("/")
def home():
    return {
        "message": "Notification Service Running"
    }