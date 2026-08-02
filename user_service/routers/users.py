import logging
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Request, status
from sqlalchemy import or_
from sqlalchemy.orm import Session

from auth import (
    create_access_token,
    hash_password,
    verify_password,
)
from database import get_db
from kafka_producer import publish_user_event
from models import User
from schemas import (
    TokenResponse,
    UserLogin,
    UserRegister,
    UserResponse,
)

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/users",
    tags=["Authentication"],
)


@router.post(
    "/register",
    response_model=UserResponse,
    status_code=status.HTTP_201_CREATED,
)
def register_user(
    user_data: UserRegister,
    db: Session = Depends(get_db),
):
    logger.info(
        "Registration request received for email=%s",
        user_data.email,
    )

    # Check if email or phone number already exists
    user = (
        db.query(User)
        .filter(
            or_(
                User.email == user_data.email,
                User.phone_number == user_data.phone_number,
            )
        )
        .first()
    )

    if user:
        logger.warning(
            "Registration failed: User already exists (email=%s)",
            user_data.email,
        )

        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="User already exists",
        )

    # Hash password
    hashed_password = hash_password(user_data.password)

    # Create user
    new_user = User(
        email=user_data.email,
        phone_number=user_data.phone_number,
        hashed_password=hashed_password,
        notification_preference=user_data.notification_preference,
    )

    # Save to database
    db.add(new_user)
    db.commit()
    db.refresh(new_user)

    # Publish Kafka event
    try:
        publish_user_event(
            {
                "event": "USER_REGISTERED",
                "user_id": str(new_user.id),
                "email": new_user.email,
                "phone_number": new_user.phone_number,
                "notification_preference": new_user.notification_preference.value,
            }
        )
        logger.info(
            "USER_REGISTERED event published for user_id=%s",
            new_user.id,
        )
    except Exception:
        logger.exception(
            "Failed to publish USER_REGISTERED event for user_id=%s",
            new_user.id,
        )

    logger.info(
        "User registered successfully: user_id=%s",
        new_user.id,
    )

    return new_user


@router.post(
    "/login",
    response_model=TokenResponse,
)
def login_user(
    login_data: UserLogin,
    db: Session = Depends(get_db),
):
    logger.info(
        "Login request received for email=%s",
        login_data.email,
    )

    # Find user
    user = (
        db.query(User)
        .filter(User.email == login_data.email)
        .first()
    )

    if not user:
        logger.warning(
            "Login failed: invalid credentials for email=%s",
            login_data.email,
        )

        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid email or password",
        )

    # Verify password
    if not verify_password(
        login_data.password,
        user.hashed_password,
    ):
        logger.warning(
            "Login failed: invalid credentials for email=%s",
            login_data.email,
        )

        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid email or password",
        )

    # Generate JWT
    access_token = create_access_token(user.id)

    logger.info(
        "User logged in successfully: user_id=%s",
        user.id,
    )

    return {
        "access_token": access_token,
        "token_type": "bearer",
    }


@router.get(
    "/profile",
    response_model=UserResponse,
)
def get_profile(
    request: Request,
    db: Session = Depends(get_db),
):
    user_id = getattr(request.state, "user_id", None)

    if user_id is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated",
        )

    user = (
        db.query(User)
        .filter(User.id == UUID(user_id))
        .first()
    )

    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found",
        )

    return user


@router.get(
    "/{user_id}",
    response_model=UserResponse,
)
def get_user(
    user_id: UUID,
    db: Session = Depends(get_db),
):
    user = (
        db.query(User)
        .filter(User.id == user_id)
        .first()
    )

    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found",
        )

    return user