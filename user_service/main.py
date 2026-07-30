import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from database import Base, engine
import models
from routers import users


logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Initializing database tables")

    Base.metadata.create_all(bind=engine)

    logger.info("Database tables initialized")
    logger.info("User Service started successfully")

    yield

    logger.info("User Service shutting down")


app = FastAPI(
    title="User Service",
    lifespan=lifespan
)


# Register routes from routers/users.py
app.include_router(users.router)


@app.get("/")
def root():
    return {
        "message": "User service is running"
    }