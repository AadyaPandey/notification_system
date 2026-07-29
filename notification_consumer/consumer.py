from kafka import KafkaConsumer
import json
import time

from notification_service.database import SessionLocal
from notification_service.models import Notification, NotificationStatus

from .retry import retry


consumer = KafkaConsumer(
    "notifications.created",
    "notifications.retry",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)


def send_email(notification):
    """
    Simulates sending an email.
    """

    print("\nSending Email...")

    # Simulate email sending time
    time.sleep(2)

    # Uncomment this line to test retry
    raise Exception("SMTP Server Down")

    print("Email sent successfully!")


print("Listening for notifications...")


for message in consumer:

    notification_id = message.value["notification_id"]

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

        print("\n========== Notification ==========")
        print(f"ID      : {notification.id}")
        print(f"User    : {notification.user_id}")
        print(f"Title   : {notification.title}")
        print(f"Message : {notification.message}")
        print(f"Channel : {notification.channel}")
        print(f"Status  : {notification.status}")
        print("==================================")

        # Retry email sending
        retry(lambda: send_email(notification))

        # Update status only if email succeeds
        notification.status = NotificationStatus.SENT
        db.commit()

        print("Status updated to SENT")

    except Exception as e:
        print(f"\nNotification processing failed: {e}")

    finally:
        db.close()