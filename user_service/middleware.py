import jwt

from fastapi import Request
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

from .auth import JWT_SECRET_KEY, JWT_ALGORITHM


class JWTMiddleware(BaseHTTPMiddleware):

    async def dispatch(self, request: Request, call_next):

        # Routes that REQUIRE authentication
        protected_routes = [
            "/users/profile",
        ]

        # If route is not protected, continue normally
        if request.url.path not in protected_routes:
            return await call_next(request)

        # Get Authorization header
        authorization = request.headers.get("Authorization")

        if not authorization:
            return JSONResponse(
                status_code=401,
                content={"detail": "Authorization header missing"}
            )

        try:
            # Expected:
            # Authorization: Bearer <token>
            scheme, token = authorization.split(" ", 1)

            if scheme.lower() != "bearer":
                return JSONResponse(
                    status_code=401,
                    content={"detail": "Invalid authentication scheme"}
                )

            # Verify and decode JWT
            payload = jwt.decode(
                token,
                JWT_SECRET_KEY,
                algorithms=[JWT_ALGORITHM]
            )

            # Extract user ID
            user_id = payload.get("sub")

            if not user_id:
                return JSONResponse(
                    status_code=401,
                    content={"detail": "Invalid token"}
                )

            # Make user ID available to the endpoint
            request.state.user_id = user_id

        except jwt.ExpiredSignatureError:
            return JSONResponse(
                status_code=401,
                content={"detail": "Token expired"}
            )

        except jwt.InvalidTokenError:
            return JSONResponse(
                status_code=401,
                content={"detail": "Invalid token"}
            )

        except ValueError:
            return JSONResponse(
                status_code=401,
                content={"detail": "Invalid authorization header"}
            )

        # JWT valid → continue to /auth/profile
        return await call_next(request)