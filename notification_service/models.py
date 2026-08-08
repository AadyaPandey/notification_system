import enum
import uuid

from sqlalchemy import Column, DateTime, Enum, Integer, String, Text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import func

from database import Base


class NotificationChannel(str, enum.Enum):
    EMAIL = "EMAIL"
    SMS = "SMS"
    PUSH = "PUSH"


class NotificationStatus(str, enum.Enum):
    PENDING = "PENDING"
    SENT = "SENT"
    RETRY_PENDING = "RETRY_PENDING"
    FAILED = "FAILED"


class Notification(Base):
    __tablename__ = "notifications"

    id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4
    )

    user_id = Column(
        UUID(as_uuid=True),
        nullable=False
    )

    recipient = Column(
        String(255),
        nullable=False
    )

    subject = Column(
        String(255),
        nullable=False
    )

    message = Column(
        Text,
        nullable=False
    )

    channel = Column(
        Enum(NotificationChannel),
        nullable=False
    )

    status = Column(
        Enum(NotificationStatus),
        default=NotificationStatus.PENDING,
        nullable=False
    )

    created_at = Column(
        DateTime(timezone=True),
        server_default=func.now()
    )

    updated_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now()
    )
    retry_count = Column(
        Integer,
        default=0,
        nullable=False
    )

    next_retry_at = Column(
        DateTime(timezone=True),
        nullable=True
    )

    last_error = Column(
        Text,
        nullable=True
    )

class NotificationUser(Base):
    __tablename__ = "notification_users"

    user_id = Column(
        UUID(as_uuid=True),
        primary_key=True
    )

    email = Column(
        String(255),
        nullable=False
    )

    phone_number = Column(
        String(10),
        nullable=False
    )

    notification_preference = Column(
        String(50),
        nullable=False
    )

    created_at = Column(
        DateTime(timezone=True),
        server_default=func.now()
    )

    updated_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now()
    )