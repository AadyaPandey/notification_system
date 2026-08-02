import logging

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from kafka_producer import publish_user_event

from database import get_db
from models import User
from schemas import (
    UserRegister,
    UserResponse,
    UserLogin,
    TokenResponse
)

from auth import (
    hash_password,
    verify_password,
    create_access_token
)
from uuid import UUID
from fastapi import Request


logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/users",
    tags=["Authentication"]
)


@router.post(
    "/register",
    response_model=UserResponse,
    status_code=status.HTTP_201_CREATED
)
def register_user(
    user_data: UserRegister,
    db: Session = Depends(get_db)
):
    logger.info(
        "Registration request received for email=%s",
        user_data.email
    )

    # 1. Check if user already exists
    existing_user = (
        db.query(User)
        .filter(User.email == user_data.email)
        .first()
    )

    if existing_user:
        logger.warning(
            "Registration failed: email already registered"
        )

        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Email already registered"
        )

    # 2. Hash the password
    hashed_password = hash_password(user_data.password)

    # 3. Create User object
    new_user = User(
        email=user_data.email,
        hashed_password=hashed_password,
        notification_preference=user_data.notification_preference
    )

    # 4. Save user to PostgreSQL
    db.add(new_user)
    db.commit()
    db.refresh(new_user)

    publish_user_event(
    {
        "event": "USER_REGISTERED",
        "user_id": str(new_user.id),
        "email": new_user.email,
        "notification_preference": new_user.notification_preference.value,
    }
)

    logger.info(
        "User registered successfully: user_id=%s",
        new_user.id
    )

    # 5. Return user
    return new_user

@router.post(
    "/login",
    response_model=TokenResponse
)
def login_user(
    login_data: UserLogin,
    db: Session = Depends(get_db)
):

    logger.info(
        "Login request received for email=%s",
        login_data.email
    )

    # 1. Find user
    user = (
        db.query(User)
        .filter(User.email == login_data.email)
        .first()
    )

    # 2. Check if user exists
    if not user:
        logger.warning("Login failed: invalid credentials")

        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid email or password"
        )

    # 3. Verify password
    if not verify_password(
        login_data.password,
        user.hashed_password
    ):
        logger.warning("Login failed: invalid credentials")

        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid email or password"
        )

    # 4. Generate JWT
    access_token = create_access_token(user.id)

    logger.info(
        "User logged in successfully: user_id=%s",
        user.id
    )

    # 5. Return JWT
    return {
        "access_token": access_token,
        "token_type": "bearer"
    }

@router.get(
    "/profile",
    response_model=UserResponse
)
def get_profile(
    request: Request,
    db: Session = Depends(get_db)
):
    user_id = request.state.user_id

    user = (
        db.query(User)
        .filter(User.id == UUID(user_id))
        .first()
    )

    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )

    return user


@router.get(
    "/{user_id}",
    response_model=UserResponse
)
def get_user(
    user_id: UUID,
    db: Session = Depends(get_db)
):
    user = (
        db.query(User)
        .filter(User.id == user_id)
        .first()
    )

    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )

    return user