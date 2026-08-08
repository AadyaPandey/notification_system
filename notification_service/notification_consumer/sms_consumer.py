import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone

from prometheus_client import Counter, start_http_server
from kafka import KafkaConsumer

from database import SessionLocal
from models import Notification, NotificationStatus


# --------------------------------------------------
# Logging configuration
# --------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

logger = logging.getLogger("sms-consumer")


# --------------------------------------------------
# Retry configuration
# --------------------------------------------------

MAX_RETRIES = 3
INITIAL_RETRY_DELAY = 5


# --------------------------------------------------
# Prometheus metrics
# --------------------------------------------------

sms_processed = Counter(
    "sms_notifications_processed_total",
    "Total number of SMS notifications successfully processed",
)

sms_failed = Counter(
    "sms_notifications_failed_total",
    "Total number of SMS notifications that failed",
)


# --------------------------------------------------
# SMS sending
# --------------------------------------------------

def send_sms(notification):
    """
    Simulate sending an SMS through a Twilio-style provider.

    No real Twilio account or API call is required.
    The SMS is intentionally forced to fail so that
    the retry and DLQ flow can be tested.
    """

    logger.info(
        "Sending SMS to %s",
        notification.recipient,
    )

    time.sleep(2)

    logger.info("SMS Provider: Twilio")
    logger.info("Connecting to SMS provider...")
    logger.info("Preparing SMS request...")
    logger.info("To: %s", notification.recipient)

    # --------------------------------------------------
    # REAL TWILIO CODE WOULD LOOK LIKE:
    #
    # twilio_client.messages.create(
    #     body=notification.message,
    #     from_=TWILIO_PHONE_NUMBER,
    #     to=notification.recipient
    # )
    #
    # We are NOT calling Twilio because we don't
    # have an account or credentials.
    # --------------------------------------------------

    # Force failure every time
    raise Exception(
        "Twilio SMS delivery failed "
        "(simulated failure for retry/DLQ testing)"
    )


# --------------------------------------------------
# Retry scheduling
# --------------------------------------------------

def schedule_retry(db, notification, error):
    """
    Schedule the next retry using exponential backoff.

    Retry 1 -> 5 seconds
    Retry 2 -> 10 seconds
    Retry 3 -> 20 seconds
    After that -> FAILED / DLQ
    """

    notification.retry_count += 1
    notification.last_error = str(error)

    # --------------------------------------------------
    # Schedule another retry
    # --------------------------------------------------

    if notification.retry_count <= MAX_RETRIES:

        delay_seconds = INITIAL_RETRY_DELAY * (
            2 ** (notification.retry_count - 1)
        )

        notification.status = NotificationStatus.RETRY_PENDING

        notification.next_retry_at = (
            datetime.now(timezone.utc)
            + timedelta(seconds=delay_seconds)
        )

        db.commit()

        logger.warning(
            "SMS notification %s scheduled for retry | "
            "retry_count=%d/%d | delay=%ds | next_retry_at=%s",
            notification.id,
            notification.retry_count,
            MAX_RETRIES,
            delay_seconds,
            notification.next_retry_at,
        )

    # --------------------------------------------------
    # Maximum retries reached
    # --------------------------------------------------

    else:

        notification.status = NotificationStatus.FAILED
        notification.next_retry_at = None

        db.commit()

        logger.error(
            "SMS notification %s permanently failed "
            "after %d retries",
            notification.id,
            MAX_RETRIES,
        )


# --------------------------------------------------
# Main consumer
# --------------------------------------------------

def main() -> None:

    logger.info("Starting SMS consumer...")

    # --------------------------------------------------
    # Start Prometheus metrics server
    # --------------------------------------------------

    start_http_server(9100)

    logger.info(
        "Prometheus metrics server started on port 9100"
    )

    # --------------------------------------------------
    # Kafka consumer
    # --------------------------------------------------

    consumer = KafkaConsumer(
        "notifications.sms",
        bootstrap_servers=os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS"
        ),
        group_id="sms-group",
        auto_offset_reset="latest",
        value_deserializer=lambda m: json.loads(
            m.decode("utf-8")
        ),
    )

    logger.info(
        "Connected to Kafka topic=notifications.sms"
    )

    # --------------------------------------------------
    # Consume messages
    # --------------------------------------------------

    for message in consumer:

        event = message.value

        logger.info(
            "Received SMS event: %s",
            event,
        )

        notification_id = event["notification_id"]

        db = SessionLocal()

        try:

            notification = (
                db.query(Notification)
                .filter(
                    Notification.id == notification_id
                )
                .first()
            )

            # --------------------------------------------------
            # Notification doesn't exist
            # --------------------------------------------------

            if notification is None:

                logger.error(
                    "Notification not found | "
                    "notification_id=%s",
                    notification_id,
                )

                continue

            logger.info(
                "Processing notification | "
                "notification_id=%s | "
                "retry_count=%s",
                notification.id,
                notification.retry_count,
            )

            # --------------------------------------------------
            # Prevent duplicate delivery
            # --------------------------------------------------

            if notification.status == NotificationStatus.SENT:

                logger.info(
                    "Notification %s already SENT. "
                    "Skipping duplicate delivery.",
                    notification.id,
                )

                continue

            # --------------------------------------------------
            # Already permanently failed
            # --------------------------------------------------

            if notification.status == NotificationStatus.FAILED:

                logger.info(
                    "Notification %s already FAILED. "
                    "Skipping.",
                    notification.id,
                )

                continue

            # --------------------------------------------------
            # Attempt SMS delivery
            # --------------------------------------------------

            try:

                send_sms(notification)

                # --------------------------------------------------
                # SUCCESS
                # --------------------------------------------------

                notification.status = NotificationStatus.SENT
                notification.next_retry_at = None
                notification.last_error = None

                db.commit()

                sms_processed.inc()

                logger.info(
                    "SMS notification %s marked SENT",
                    notification.id,
                )

            except Exception as exc:

                # --------------------------------------------------
                # FAILURE
                # --------------------------------------------------

                sms_failed.inc()

                logger.exception(
                    "SMS delivery failed | "
                    "notification_id=%s",
                    notification.id,
                )

                # Schedule retry in database
                schedule_retry(
                    db=db,
                    notification=notification,
                    error=exc,
                )

        except Exception:

            db.rollback()

            logger.exception(
                "Unexpected error while processing "
                "SMS notification"
            )

        finally:

            db.close()

            logger.debug(
                "Database session closed"
            )


# --------------------------------------------------
# Entry point
# --------------------------------------------------

if __name__ == "__main__":
    main()