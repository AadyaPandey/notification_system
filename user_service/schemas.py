from pydantic import BaseModel, EmailStr, Field
from uuid import UUID
from datetime import datetime

from .models import NotificationPreference


class UserRegister(BaseModel):
    email: EmailStr
    password: str = Field(min_length=8)
    notification_preference: NotificationPreference = NotificationPreference.EMAIL


class UserLogin(BaseModel):
    email: EmailStr
    password: str


class UserResponse(BaseModel):
    id: UUID
    email: EmailStr
    notification_preference: NotificationPreference
    created_at: datetime
    updated_at: datetime

    model_config = {
        "from_attributes": True
    }

class TokenResponse(BaseModel):
    access_token: str
    token_type: str