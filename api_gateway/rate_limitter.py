from fastapi import HTTPException
from redis_client import redis_client

LIMIT = 10
WINDOW = 60


def check_rate_limit(key: str, limit=LIMIT, window=WINDOW):

    count = redis_client.incr(key)

    if count == 1:
        redis_client.expire(key, window)

    if count > limit:
        raise HTTPException(
            status_code=429,
            detail="Rate limit exceeded"
        )