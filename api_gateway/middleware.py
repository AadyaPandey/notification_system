import jwt

from fastapi import Request
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

from auth import JWT_SECRET_KEY, JWT_ALGORITHM


class JWTMiddleware(BaseHTTPMiddleware):

    async def dispatch(self, request: Request, call_next):

        # Allow CORS preflight requests
        if request.method == "OPTIONS":
            return await call_next(request)

        public_routes = [
            "/users/register",
            "/users/login",
        ]

        if request.url.path in public_routes:
            return await call_next(request)

        protected_routes = [
            "/users/profile",
            "/notifications",
        ]

        is_protected = any(
            request.url.path.startswith(route)
            for route in protected_routes
        )

        if not is_protected:
            return await call_next(request)

        authorization = request.headers.get("Authorization")

        if not authorization:
            return JSONResponse(
                status_code=401,
                content={"detail": "Authorization header missing"},
            )

        try:
            scheme, token = authorization.split(" ", 1)

            if scheme.lower() != "bearer":
                return JSONResponse(
                    status_code=401,
                    content={"detail": "Invalid authentication scheme"},
                )

            payload = jwt.decode(
                token,
                JWT_SECRET_KEY,
                algorithms=[JWT_ALGORITHM],
            )

            user_id = payload.get("sub")

            if not user_id:
                return JSONResponse(
                    status_code=401,
                    content={"detail": "Invalid token"},
                )

            request.state.user_id = user_id

        except jwt.ExpiredSignatureError:
            return JSONResponse(
                status_code=401,
                content={"detail": "Token expired"},
            )

        except jwt.InvalidTokenError:
            return JSONResponse(
                status_code=401,
                content={"detail": "Invalid token"},
            )

        except ValueError:
            return JSONResponse(
                status_code=401,
                content={"detail": "Invalid Authorization header"},
            )

        return await call_next(request)