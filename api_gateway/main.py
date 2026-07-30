from fastapi import FastAPI

from middleware import JWTMiddleware

from routes.public import router as public_router
from routes.private import router as private_router


app = FastAPI(title="API Gateway")


app.add_middleware(JWTMiddleware)

app.include_router(public_router)
app.include_router(private_router)