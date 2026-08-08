import json
import logging
import os
import random
import time

from prometheus_client import Counter, start_http_server
from kafka import KafkaConsumer

from database import SessionLocal
from models import Notification, NotificationStatus
from kafka_producer import publish_event


# --------------------------------------------------
# Logging configuration
# --------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

logger = logging.getLogger("retry-consumer")


# --------------------------------------------------
# Configuration
# --------------------------------------------------

MAX_RETRY_COUNT = 3


# --------------------------------------------------
# Prometheus metrics
# --------------------------------------------------

retry_processed = Counter(
    "notification_retry_processed_total",
    "Total number of retry events successfully processed",
    ["channel"],
)

retry_failed = Counter(
    "notification_retry_failed_total",
    "Total number of retry attempts that failed",
    ["channel"],
)

notifications_dlq = Counter(
    "notifications_dlq_total",
    "Total number of notifications moved to the dead letter queue",
    ["channel"],
)


# --------------------------------------------------
# Retry backoff
# --------------------------------------------------

def get_backoff_seconds(retry_count: int) -> int:
    """Return exponential backoff delay in seconds for retry attempts."""
    return min(2 ** max(retry_count - 1, 0), 60)


# --------------------------------------------------
# Notification providers
# --------------------------------------------------

def send_email(notification):
    logger.info(
        "Retrying Email | recipient=%s",
        notification.recipient,
    )

    time.sleep(2)

    if random.choice([True, False]):
        raise Exception("Email service unavailable")

    logger.info(
        "Email Sent | recipient=%s",
        notification.recipient,
    )


def send_sms(notification):
    logger.info(
        "Retrying SMS | recipient=%s",
        notification.recipient,
    )

    time.sleep(2)

    if random.choice([True, False]):
        raise Exception("SMS Gateway unavailable")

    logger.info(
        "SMS Sent | recipient=%s",
        notification.recipient,
    )


def send_push(notification):
    logger.info(
        "Retrying Push Notification | recipient=%s",
        notification.recipient,
    )

    time.sleep(2)

    if random.choice([True, False]):
        raise Exception("Push service unavailable")

    logger.info(
        "Push Sent | recipient=%s",
        notification.recipient,
    )


# --------------------------------------------------
# Main consumer
# --------------------------------------------------

def main() -> None:

    logger.info("Starting retry consumer...")

    # Start Prometheus metrics server
    start_http_server(9100)

    logger.info(
        "Prometheus metrics server started on port 9100"
    )

    consumer = KafkaConsumer(
        "notifications.retry",
        bootstrap_servers=os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS"
        ),
        group_id="retry-group",
        auto_offset_reset="latest",
        value_deserializer=lambda m: json.loads(
            m.decode("utf-8")
        ),
    )

    logger.info(
        "Connected to Kafka topic=notifications.retry"
    )

    for message in consumer:

        event = message.value

        logger.info(
            "Received retry event | event=%s",
            event,
        )

        notification_id = event["notification_id"]
        retry_count = event["retry_count"]
        user_id = event["user_id"]
        channel = event["channel"]

        db = SessionLocal()

        try:

            notification = (
                db.query(Notification)
                .filter(
                    Notification.id == notification_id
                )
                .first()
            )

            if notification is None:

                logger.error(
                    "Notification not found | notification_id=%s",
                    notification_id,
                )

                continue

            logger.info(
                "Processing retry | "
                "notification_id=%s | "
                "channel=%s | "
                "retry_count=%s",
                notification.id,
                channel,
                retry_count,
            )

            # --------------------------------------------------
            # Prevent duplicate processing
            # --------------------------------------------------

            if notification.status == NotificationStatus.SENT:

                logger.warning(
                    "Notification already sent. "
                    "Skipping duplicate retry | "
                    "notification_id=%s",
                    notification.id,
                )

                continue

            try:

                # --------------------------------------------------
                # Send notification based on channel
                # --------------------------------------------------

                if channel == "EMAIL":
                    send_email(notification)

                elif channel == "SMS":
                    send_sms(notification)

                elif channel == "PUSH":
                    send_push(notification)

                else:
                    raise Exception(
                        f"Invalid channel: {channel}"
                    )

                # --------------------------------------------------
                # Success
                # --------------------------------------------------

                notification.status = NotificationStatus.SENT

                db.commit()

                retry_processed.labels(
                    channel=channel
                ).inc()

                logger.info(
                    "Notification SENT successfully | "
                    "notification_id=%s | "
                    "channel=%s",
                    notification.id,
                    channel,
                )

            except Exception:

                # Count failed retry attempt
                retry_failed.labels(
                    channel=channel
                ).inc()

                logger.exception(
                    "Notification delivery failed | "
                    "notification_id=%s | "
                    "channel=%s | "
                    "retry_count=%s",
                    notification.id,
                    channel,
                    retry_count,
                )

                retry_count += 1

                # --------------------------------------------------
                # Retry
                # --------------------------------------------------

                if retry_count < MAX_RETRY_COUNT:

                    backoff_seconds = get_backoff_seconds(
                        retry_count
                    )

                    logger.warning(
                        "Retry scheduled | "
                        "notification_id=%s | "
                        "retry_count=%s/%s | "
                        "backoff=%ss",
                        notification.id,
                        retry_count,
                        MAX_RETRY_COUNT,
                        backoff_seconds,
                    )

                    time.sleep(backoff_seconds)

                    publish_event(
                        topic="notifications.retry",
                        user_id=user_id,
                        notification_id=notification.id,
                        channel=channel,
                        retry_count=retry_count,
                    )

                    logger.info(
                        "Retry event published | "
                        "notification_id=%s | "
                        "retry_count=%s",
                        notification.id,
                        retry_count,
                    )

                # --------------------------------------------------
                # Maximum retries reached → DLQ
                # --------------------------------------------------

                else:

                    notification.status = (
                        NotificationStatus.FAILED
                    )

                    db.commit()

                    notifications_dlq.labels(
                        channel=channel
                    ).inc()

                    logger.error(
                        "Maximum retries reached | "
                        "notification_id=%s | "
                        "retry_count=%s | "
                        "Moving to DLQ",
                        notification.id,
                        retry_count,
                    )

                    publish_event(
                        topic="notifications.dlq",
                        user_id=user_id,
                        notification_id=notification.id,
                        channel=channel,
                        retry_count=retry_count,
                    )

                    logger.info(
                        "Notification moved to DLQ | "
                        "notification_id=%s | "
                        "channel=%s",
                        notification.id,
                        channel,
                    )

        finally:

            db.close()

            logger.debug(
                "Database session closed | notification_id=%s",
                notification_id,
            )


if __name__ == "__main__":
    main()

