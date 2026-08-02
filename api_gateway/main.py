from fastapi import FastAPI

from middleware import JWTMiddleware
from routes.public import router as public_router
from routes.private import router as private_router
from fastapi.middleware.cors import CORSMiddleware


app = FastAPI(title="API Gateway")

app.add_middleware(JWTMiddleware)

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



app.include_router(public_router)
app.include_router(private_router)