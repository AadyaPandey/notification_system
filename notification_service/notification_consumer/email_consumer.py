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
    print("[1] Entered send_email()")

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

    print(f"[2] Resend responded with status {response.status_code}")

    if response.status_code not in (200, 201):
        print("[3] Email request failed")
        raise Exception(
            f"Resend Error ({response.status_code}): {response.text}"
        )

    print(f"[4] Email sent successfully to {notification.recipient}")


def main() -> None:
    print("[A] Starting email consumer")

    consumer = KafkaConsumer(
        "notifications.email",
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
        group_id="email-group",
        auto_offset_reset="latest",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )

    print("[B] Kafka consumer created. Waiting for messages...")

    for message in consumer:
        print("[C] Received a Kafka message")

        event = message.value
        print(f"[D] Event: {event}")

        notification_id = event["notification_id"]
        retry_count = event["retry_count"]
        user_id = event["user_id"]
        channel = event.get("channel", "EMAIL")

        db = SessionLocal()
        print("[E] Database session created")

        try:
            notification = (
                db.query(Notification)
                .filter(Notification.id == notification_id)
                .first()
            )

            print("[F] Database query executed")

            if notification is None:
                print("[G] Notification not found")
                continue

            print(f"[H] Notification found. Status={notification.status}")

            if notification.status == NotificationStatus.SENT:
                print(
                    f"[I] Notification {notification.id} already sent. Skipping."
                )
                continue

            print("[J] Calling send_email()")

            try:
                send_email(notification)

                print("[K] Returned from send_email()")

                notification.status = NotificationStatus.SENT
                print("[L] Status updated to SENT")

                db.commit()
                print("[M] Database committed")

                print(f"[N] Notification {notification.id} marked SENT")

            except Exception as e:
                print(f"[O] Exception while sending email: {e}")

                retry_count += 1
                print(f"[P] Retry count = {retry_count}")

                publish_event(
                    topic="notifications.retry",
                    user_id=user_id,
                    notification_id=notification.id,
                    channel=channel,
                    retry_count=retry_count,
                )

                print(
                    f"[Q] Published retry event for notification {notification.id}"
                )

        finally:
            print("[R] Closing DB session")
            db.close()


if __name__ == "__main__":
    main()

