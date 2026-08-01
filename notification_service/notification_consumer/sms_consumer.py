import json
import os
import random
import time

from kafka import KafkaConsumer

from database import SessionLocal
from models import Notification, NotificationStatus
from kafka_producer import publish_event


def send_sms(notification):
    """
    Simulate sending an SMS.
    """

    print(
        f"\nSending SMS to {notification.recipient}"
    )

    time.sleep(2)

    # Simulate random failure
    if random.choice([True, False]):
        raise Exception("SMS Gateway Unavailable")

    print("SMS sent successfully!")


def main() -> None:
    consumer = KafkaConsumer(
        "notifications.sms",
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
        group_id="sms-group",
        auto_offset_reset="latest",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )

    for message in consumer:
        event = message.value

        notification_id = event["notification_id"]
        retry_count = event["retry_count"]
        user_id = event["user_id"]
        channel = event.get("channel", "SMS")

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
                send_sms(notification)

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

