import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from .database import Base, engine
from . import models
from .routers import users
from .middleware import JWTMiddleware


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

app.add_middleware(JWTMiddleware)

# Register routes from routers/users.py
app.include_router(users.router)


@app.get("/")
def root():
    return {
        "message": "User service is running"
    }