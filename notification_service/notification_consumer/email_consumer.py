import json
import os
import random
import time

from kafka import KafkaConsumer

from database import SessionLocal
from models import Notification, NotificationStatus
from kafka_producer import publish_event
import requests

RESEND_API_KEY = os.getenv("RESEND_API_KEY")


def send_email(notification):
    """
    Sends an email using Resend.
    Raises an exception if the request fails.
    """

    response = requests.post(
        "https://api.resend.com/emails",
        headers={
            "Authorization": f"Bearer {RESEND_API_KEY}",
            "Content-Type": "application/json",
            "Idempotency-Key": str(notification.id),
        },
        json={
            "from": "aadyapandey2004@gmail.com",   
            "to": notification.recipient,
            "subject": notification.subject,
            "html": notification.message,
        },
        timeout=10,
    )

    if response.status_code not in (200, 201):
        raise Exception(
            f"Resend Error ({response.status_code}): {response.text}"
        )

    print(f"Email sent successfully to {notification.recipient}")


def main() -> None:
    consumer = KafkaConsumer(
        "notifications.email",
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
        group_id="email-group",
        auto_offset_reset="latest",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )

    for message in consumer:
        event = message.value

        notification_id = event["notification_id"]
        retry_count = event["retry_count"]
        user_id = event["user_id"]
        channel = event.get("channel", "EMAIL")

        db = SessionLocal()

        try:
            notification = (
                db.query(Notification)
                .filter(Notification.id == notification_id)
                .first()
            )

            if notification is None:
                print("Notification not found")
                continue

            if notification.status == NotificationStatus.SENT:
                print(
                    f"Notification {notification.id} already sent. Skipping duplicate delivery"
                )
                continue

            try:
                send_email(notification)

                notification.status = NotificationStatus.SENT

                db.commit()

                print(
                    f"Notification {notification.id} marked SENT"
                )

            except Exception as e:
                print(e)

                retry_count += 1

                publish_event(
                    topic="notifications.retry",
                    user_id=user_id,
                    notification_id=notification.id,
                    channel=channel,
                    retry_count=retry_count,
                )

                print(
                    f"Published retry event for notification {notification.id} with retry_count={retry_count}"
                )

        finally:
            db.close()


if __name__ == "__main__":
    main()

