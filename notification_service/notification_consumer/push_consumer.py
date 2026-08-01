import json
import os
import random
import time

from kafka import KafkaConsumer

from database import SessionLocal
from models import Notification, NotificationStatus
from kafka_producer import publish_event


def send_push(notification):
    """
    Simulate sending a Push Notification.
    """

    print(
        f"\nSending Push Notification to {notification.recipient}"
    )

    time.sleep(2)

    # Simulate random failure
    if random.choice([True, False]):
        raise Exception("Push Notification service unavailable")

    print("Push Notification sent successfully!")


def main() -> None:
    consumer = KafkaConsumer(
        "notifications.push",
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
        group_id="push-group",
        auto_offset_reset="latest",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )

    for message in consumer:
        event = message.value

        notification_id = event["notification_id"]
        retry_count = event["retry_count"]
        user_id = event["user_id"]
        channel = event.get("channel", "PUSH")

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

            try:
                send_push(notification)

                notification.status = NotificationStatus.SENT

                db.commit()

                print(
                    f"Notification {notification.id} marked SENT"
                )

            except Exception as e:
                print(e)

                retry_count += 1

                if retry_count < 3:
                    publish_event(
                        topic="notifications.retry",
                        user_id=user_id,
                        notification_id=notification.id,
                        channel=channel,
                        retry_count=retry_count,
                    )

                    print("Published to Retry Topic")

                else:
                    notification.status = NotificationStatus.FAILED
                    db.commit()

                    publish_event(
                        topic="notifications.dlq",
                        user_id=user_id,
                        notification_id=notification.id,
                        channel=channel,
                        retry_count=retry_count,
                    )

                    print("Published to DLQ")

        finally:
            db.close()


if __name__ == "__main__":
    main()

