import json
import random
import time

from kafka import KafkaConsumer

from database import SessionLocal
from models import Notification, NotificationStatus
from kafka_producer import publish_event


import os

consumer = KafkaConsumer(
    "notifications.created",
    "notifications.retry",
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
    auto_offset_reset="earliest",
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)


def send_email(notification):
    """
    Simulate sending an email.
    Randomly fails to demonstrate retries.
    """

    print(f"Sending email to {notification.recipient}")

    time.sleep(2)

    # Simulate random failure
    if random.choice([True, False]):
        raise Exception("Email service unavailable")

    print("Email sent successfully!")


for message in consumer:

    notification_id = message.value["notification_id"]
    retry_count = message.value["retry_count"]

    db = SessionLocal()

    try:
        notification = (
            db.query(Notification)
            .filter(Notification.id == notification_id)
            .first()
        )

        if notification is None:
            print(f"Notification {notification_id} not found")
            continue

        print(
            f"\nReceived Notification "
            f"{notification.id} "
            f"(Retry: {retry_count})"
        )

        try:
            # Temporary: only email notifications
            send_email(notification)

            notification.status = NotificationStatus.SENT
            db.commit()

            print("Notification marked as SENT")

        except Exception as e:

            print(f"Email failed: {e}")

            retry_count += 1

            if retry_count < 3:

                print(f"Retrying... Attempt {retry_count}")

                publish_event(
                    "notifications.retry",
                    notification.id,
                    retry_count
                )

            else:

                print("Maximum retries reached.")

                publish_event(
                    "notifications.dlq",
                    notification.id,
                    retry_count
                )

    finally:
        db.close()