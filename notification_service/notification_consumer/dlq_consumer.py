import json
import os

from kafka import KafkaConsumer

from database import SessionLocal
from models import Notification, NotificationStatus


def main() -> None:
    consumer = KafkaConsumer(
        "notifications.dlq",
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
        group_id="dlq-group",
        auto_offset_reset="latest",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )

    print("DLQ Consumer Started...")

    for message in consumer:
        event = message.value

        notification_id = event["notification_id"]
        retry_count = event["retry_count"]
        channel = event["channel"]

        db = SessionLocal()

        try:
            notification = (
                db.query(Notification)
                .filter(Notification.id == notification_id)
                .first()
            )

            if notification is None:
                print(f"Notification {notification_id} not found.")
                continue

            notification.status = NotificationStatus.FAILED
            db.commit()

            print("\n========== DEAD LETTER QUEUE ==========")
            print(f"Notification ID : {notification.id}")
            print(f"Channel         : {channel}")
            print(f"Recipient       : {notification.recipient}")
            print(f"Retry Attempts  : {retry_count}")
            print("Status          : FAILED")
            print("=======================================\n")

        finally:
            db.close()


if __name__ == "__main__":
    main()

