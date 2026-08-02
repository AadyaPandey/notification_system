from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from database import get_db
from models import Notification, NotificationChannel as DBChannel, NotificationUser
from schemas import NotificationCreate, NotificationResponse
from kafka_producer import publish_event

router = APIRouter(
    prefix="/notifications",
    tags=["Notifications"]
)


@router.post(
    "",
    response_model=NotificationResponse
)
def create_notification(
    notification: NotificationCreate,
    db: Session = Depends(get_db)
):

    user_id = notification.user_id
    #use this user if to get the preference form notification_db
    # Get user details from local notification DB
    user = (
        db.query(NotificationUser)
        .filter(NotificationUser.user_id == notification.user_id)
        .first()
    )

    if not user:
        raise HTTPException(
            status_code=404,
            detail="User not found"
        )

    channel_value = user.notification_preference.upper()

    if channel_value == "EMAIL":
        topic = "notifications.email"

    elif channel_value == "SMS":
        topic = "notifications.sms"

    elif channel_value == "PUSH":
        topic = "notifications.push"

    else:
        raise HTTPException(
            status_code=400,
            detail="Unsupported notification channel"
        )

    new_notification = Notification(
        user_id=user_id,
        recipient=user.email,
        subject=notification.subject,
        message=notification.message,
        channel=DBChannel(channel_value)
)

    db.add(new_notification)
    db.commit()
    db.refresh(new_notification)

    publish_event(
        topic=topic,
        user_id=user_id,
        notification_id=new_notification.id,
        channel=channel_value,
        retry_count=0
    )

    return new_notification

