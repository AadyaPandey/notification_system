import json
import logging
import os

from dotenv import load_dotenv
from prometheus_client import Counter, start_http_server
from kafka import KafkaConsumer

from database import SessionLocal
from models import NotificationUser

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# --------------------------------------------------
# Prometheus metrics
# --------------------------------------------------

user_events_processed = Counter(
    "user_events_processed_total",
    "Total number of user events successfully processed",
    ["event_type"],
)

user_events_failed = Counter(
    "user_events_failed_total",
    "Total number of user events that failed",
    ["event_type"],
)


consumer = KafkaConsumer(
    "user-events",
    bootstrap_servers=os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        "kafka:9092",
    ),
    group_id="user-consumer-group",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda m: json.loads(
        m.decode("utf-8")
    ),
)


def handle_user_registered(db, event):
    """Insert a new user into notification_users."""

    existing_user = (
        db.query(NotificationUser)
        .filter(
            NotificationUser.user_id == event["user_id"]
        )
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
        phone_number=event["phone_number"],
        notification_preference=event[
            "notification_preference"
        ],
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
        .filter(
            NotificationUser.user_id == event["user_id"]
        )
        .first()
    )

    if not user:
        logger.warning(
            "User %s not found",
            event["user_id"],
        )
        return

    user.email = event["email"]
    user.phone_number = event["phone_number"]
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

    # Start Prometheus metrics server
    start_http_server(9100)

    logger.info(
        "Prometheus metrics server started on port 9100"
    )

    for message in consumer:

        event = message.value

        logger.info(
            "Received Event: %s",
            event,
        )

        db = SessionLocal()

        event_type = event.get("event", "UNKNOWN")

        try:

            if event_type == "USER_REGISTERED":

                handle_user_registered(
                    db,
                    event,
                )

                user_events_processed.labels(
                    event_type="USER_REGISTERED"
                ).inc()

            elif event_type == "USER_UPDATED":

                handle_user_updated(
                    db,
                    event,
                )

                user_events_processed.labels(
                    event_type="USER_UPDATED"
                ).inc()

            else:

                logger.warning(
                    "Unknown event %s",
                    event_type,
                )

        except Exception:

            db.rollback()

            user_events_failed.labels(
                event_type=event_type
            ).inc()

            logger.exception(
                "Consumer Error"
            )

        finally:

            db.close()


if __name__ == "__main__":
    main()