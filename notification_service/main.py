from contextlib import asynccontextmanager

from fastapi import FastAPI

from database import Base, engine
import models
from routers import notifications
from fastapi.middleware.cors import CORSMiddleware



@asynccontextmanager
async def lifespan(app: FastAPI):

    Base.metadata.create_all(bind=engine)

    yield


app = FastAPI(
    title="Notification Service",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",  # Vite
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(notifications.router)


@app.get("/")
def home():
    return {
        "message": "Notification Service Running"
    }