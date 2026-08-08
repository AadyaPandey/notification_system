import json
import logging
import os
import random
import time
import smtplib
from prometheus_client import Counter, start_http_server
from kafka import KafkaConsumer

from database import SessionLocal
from models import Notification, NotificationStatus
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from kafka_producer import publish_event

EMAIL = os.getenv("EMAIL_ADDRESS")
APP_PASSWORD = os.getenv("EMAIL_APP_PASSWORD")

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

    if not EMAIL:
        raise ValueError("EMAIL_ADDRESS not set")
    
    if not APP_PASSWORD:
        raise ValueError("EMAIL_APP_PASSWORD not set")
    
    
    msg = MIMEMultipart("alternative")
    msg["Subject"] = notification.subject
    msg["From"] = EMAIL
    msg["To"] = notification.recipient
    
    with smtplib.SMTP("smtp.gmail.com", 587) as smtp:
        smtp.ehlo()
        smtp.starttls()
        smtp.ehlo()

        smtp.login(EMAIL, APP_PASSWORD)
    
        smtp.sendmail(
            EMAIL,
            notification.recipient,
            msg.as_string(),
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

                if retry_count <= MAX_RETRY_COUNT:
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

