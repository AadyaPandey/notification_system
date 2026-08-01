from datetime import datetime
from enum import Enum
from uuid import UUID

from pydantic import BaseModel


class NotificationChannel(str, Enum):
    EMAIL = "EMAIL"
    SMS = "SMS"
    PUSH = "PUSH"


class NotificationStatus(str, Enum):
    PENDING = "PENDING"
    SENT = "SENT"
    FAILED = "FAILED"


class NotificationCreate(BaseModel):
    user_id: UUID
    recipient: str
    subject: str
    message: str
    channel: NotificationChannel


class NotificationResponse(BaseModel):
    id: UUID
    user_id: UUID
    recipient: str
    subject: str
    message: str
    channel: NotificationChannel
    status: NotificationStatus
    created_at: datetime

    model_config = {
        "from_attributes": True
    }
