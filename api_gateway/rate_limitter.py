from fastapi import HTTPException
from redis_client import redis_client

LOGIN_LIMIT = 5
REGISTER_LIMIT = 3
NOTIFICATION_LIMIT = 30

LOGIN_WINDOW = 60
REGISTER_WINDOW = 3600
NOTIFICATION_WINDOW = 60


def check_rate_limit(key: str, limit: int, window: int):

    count = redis_client.incr(key)

    if count == 1:
        redis_client.expire(key, window)

    if count > limit:
        raise HTTPException(
            status_code=429,
            detail="Rate limit exceeded"
        )