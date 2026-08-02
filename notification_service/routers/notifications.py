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

    preference = user.notification_preference.lower()

    if preference == "both":
        channel_names = ["EMAIL", "SMS"]
    elif preference == "email":
        channel_names = ["EMAIL"]
    elif preference == "sms":
        channel_names = ["SMS"]
    else:
        raise HTTPException(
            status_code=400,
            detail="Unsupported notification channel"
        )

    created_notification = None

    for channel_value in channel_names:
        topic_map = {
            "EMAIL": "notifications.email",
            "SMS": "notifications.sms"
        }

        recipient = user.email
        if channel_value == "SMS":
            recipient = user.phone_number

        new_notification = Notification(
            user_id=user_id,
            recipient=recipient,
            subject=notification.subject,
            message=notification.message,
            channel=DBChannel(channel_value)
        )

        db.add(new_notification)
        db.commit()
        db.refresh(new_notification)

        publish_event(
            topic=topic_map[channel_value],
            user_id=user_id,
            notification_id=new_notification.id,
            channel=channel_value,
            retry_count=0
        )

        if created_notification is None:
            created_notification = new_notification

    return created_notification

