import json
import logging
import os

from dotenv import load_dotenv
from kafka import KafkaConsumer

from database import SessionLocal
from models import NotificationUser

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


consumer = KafkaConsumer(
    "user-events",
    bootstrap_servers=os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        "kafka:9092",
    ),
    group_id="user-consumer-group",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
)


def handle_user_registered(db, event):
    """Insert a new user into notification_users."""

    existing_user = (
        db.query(NotificationUser)
        .filter(NotificationUser.user_id == event["user_id"])
        .first()
    )

    if existing_user:
        logger.info(
            "User %s already exists",
            event["user_id"],
        )
        return

    user = NotificationUser(
        user_id=event["user_id"],
        email=event["email"],
        notification_preference=event["notification_preference"],
    )

    db.add(user)
    db.commit()

    logger.info(
        "Synced user %s",
        user.user_id,
    )


def handle_user_updated(db, event):
    """Update email / preference."""

    user = (
        db.query(NotificationUser)
        .filter(NotificationUser.user_id == event["user_id"])
        .first()
    )

    if not user:
        logger.warning(
            "User %s not found",
            event["user_id"],
        )
        return

    user.email = event["email"]
    user.notification_preference = event[
        "notification_preference"
    ]

    db.commit()

    logger.info(
        "Updated user %s",
        user.user_id,
    )


def main():
    logger.info("User Consumer Started...")

    for message in consumer:

        event = message.value

        logger.info(
            "Received Event: %s",
            event,
        )

        db = SessionLocal()

        try:

            event_type = event.get("event")

            if event_type == "USER_REGISTERED":
                handle_user_registered(db, event)

            elif event_type == "USER_UPDATED":
                handle_user_updated(db, event)

            else:
                logger.warning(
                    "Unknown event %s",
                    event_type,
                )

        except Exception:
            db.rollback()
            logger.exception("Consumer Error")

        finally:
            db.close()


if __name__ == "__main__":
    main()