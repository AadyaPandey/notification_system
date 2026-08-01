import httpx

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from config import USER_SERVICE, NOTIFICATION_SERVICE
from rate_limitter import (
    check_rate_limit
)

router = APIRouter(tags=["Private"])


@router.get("/users/profile")
async def get_profile(request: Request):

    user_id = request.state.user_id

    async with httpx.AsyncClient() as client:

        response = await client.get(
            f"{USER_SERVICE}/users/{user_id}"
        )

    return JSONResponse(
        status_code=response.status_code,
        content=response.json(),
    )


@router.post("/notifications")
async def create_notification(
    payload: dict,
    request: Request,
):

    user_id = request.state.user_id

    check_rate_limit(
        key=f"notification:{user_id}"
    )

    payload["user_id"] = user_id

    async with httpx.AsyncClient() as client:

        response = await client.post(
            f"{NOTIFICATION_SERVICE}/notifications",
            json=payload,
        )

    return JSONResponse(
        status_code=response.status_code,
        content=response.json(),
    )