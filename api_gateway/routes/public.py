import httpx

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from config import USER_SERVICE
from rate_limitter import (
    check_rate_limit
)

router = APIRouter(tags=["Public"])


@router.post("/users/register")
async def register(payload: dict, request: Request):

    client_ip = request.client.host

    check_rate_limit(
        key=f"register:{client_ip}"
    )

    async with httpx.AsyncClient() as client:

        response = await client.post(
            f"{USER_SERVICE}/users/register",
            json=payload,
        )

    try:
        body = response.json()
    except ValueError:
        body = {
            "detail": response.text or "Upstream user service returned an invalid response"
        }

    return JSONResponse(
        status_code=response.status_code,
        content=body,
    )


@router.post("/users/login")
async def login(payload: dict, request: Request):

    client_ip = request.client.host

    check_rate_limit(
        key=f"login:{client_ip}",
    )

    async with httpx.AsyncClient() as client:

        response = await client.post(
            f"{USER_SERVICE}/users/login",
            json=payload,
        )

    try:
        body = response.json()
    except ValueError:
        body = {
            "detail": response.text or "Upstream user service returned an invalid response"
        }

    return JSONResponse(
        status_code=response.status_code,
        content=body,
    )