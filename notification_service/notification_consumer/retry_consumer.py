import json
import os
import random
import time

from kafka import KafkaConsumer

from database import SessionLocal
from models import Notification, NotificationStatus
from kafka_producer import publish_event


MAX_RETRY_COUNT = 3


def get_backoff_seconds(retry_count: int) -> int:
    """Return exponential backoff delay in seconds for retry attempts."""
    return min(2 ** max(retry_count - 1, 0), 60)


def send_email(notification):

    print(f"Retrying Email to {notification.recipient}")

    time.sleep(2)

    if random.choice([True, False]):
        raise Exception("Email service unavailable")

    print("Email Sent")


def send_sms(notification):

    print(f"Retrying SMS to {notification.recipient}")

    time.sleep(2)

    if random.choice([True, False]):
        raise Exception("SMS Gateway unavailable")

    print("SMS Sent")


def send_push(notification):

    print(f"Retrying Push Notification to {notification.recipient}")

    time.sleep(2)

    if random.choice([True, False]):
        raise Exception("Push service unavailable")

    print("Push Sent")


def main() -> None:
    consumer = KafkaConsumer(
        "notifications.retry",
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
        group_id="retry-group",
        auto_offset_reset="latest",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )

    for message in consumer:
        event = message.value

        notification_id = event["notification_id"]
        retry_count = event["retry_count"]
        user_id = event["user_id"]
        channel = event["channel"]

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
                    f"Notification {notification.id} already sent. Skipping duplicate retry"
                )
                continue

            try:
                if channel == "EMAIL":
                    send_email(notification)
                elif channel == "SMS":
                    send_sms(notification)
                elif channel == "PUSH":
                    send_push(notification)
                else:
                    raise Exception("Invalid Channel")

                notification.status = NotificationStatus.SENT
                db.commit()

                print(f"Notification {notification.id} SENT")

            except Exception as e:
                print(e)

                retry_count += 1

                if retry_count < MAX_RETRY_COUNT:
                    backoff_seconds = get_backoff_seconds(retry_count)
                    print(
                        f"Backing off for {backoff_seconds}s before retry {retry_count}"
                    )
                    time.sleep(backoff_seconds)

                    publish_event(
                        topic="notifications.retry",
                        user_id=user_id,
                        notification_id=notification.id,
                        channel=channel,
                        retry_count=retry_count,
                    )

                    print(f"Retry {retry_count} Published")

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

                    print("Moved to DLQ")

        finally:
            db.close()


if __name__ == "__main__":
    main()

