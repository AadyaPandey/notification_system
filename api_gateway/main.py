from fastapi import FastAPI

from middleware import JWTMiddleware
from fastapi.middleware.cors import CORSMiddleware
from routes.public import router as public_router
from routes.private import router as private_router


app = FastAPI(title="API Gateway")

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",  # Vite
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.add_middleware(JWTMiddleware)

app.include_router(public_router)
app.include_router(private_router)