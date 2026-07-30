from fastapi import APIRouter, Depends, Request, HTTPException
from sqlalchemy.orm import Session

from database import get_db
from models import Notification
from schemas import NotificationCreate, NotificationResponse
from kafka_producer import publish_event

router = APIRouter(
    prefix="/notifications",
    tags=["Notifications"]
)

@router.post("", response_model=NotificationResponse)
def create_notification(
    request: Request,
    notification: NotificationCreate,
    db: Session = Depends(get_db)
):

    user_id = notification.user_id

    new_notification = Notification(
        user_id=user_id,
        title=notification.title,
        message=notification.message,
        channel=notification.channel
    )

    db.add(new_notification)
    db.commit()
    db.refresh(new_notification)

    publish_event(
    "notifications.created",
    notification.id
    )

    return new_notification

@router.get("", response_model=list[NotificationResponse])
def get_notifications(
    request: Request,
    db: Session = Depends(get_db)
):

    user_id = request.state.user_id

    notifications = (
        db.query(Notification)
        .filter(Notification.user_id == user_id)
        .all()
    )

    return notifications

from uuid import UUID

@router.get("/{notification_id}", response_model=NotificationResponse)
def get_notification(
    notification_id: UUID,
    request: Request,
    db: Session = Depends(get_db)
):

    user_id = request.state.user_id

    notification = (
        db.query(Notification)
        .filter(
            Notification.id == notification_id,
            Notification.user_id == user_id
        )
        .first()
    )

    if notification is None:
        raise HTTPException(
            status_code=404,
            detail="Notification not found"
        )

    return notification