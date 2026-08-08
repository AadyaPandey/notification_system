import json
import os
import time
import logging

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

logger = logging.getLogger("sms-consumer")


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

    # Simulate Twilio-style SMS provider
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
# Main consumer
# --------------------------------------------------

def main() -> None:

    logger.info("Starting SMS consumer...")

    # Start Prometheus metrics server
    start_http_server(9100)

    logger.info(
        "Prometheus metrics server started on port 9100"
    )

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

    for message in consumer:

        event = message.value

        logger.info(
            "Received SMS event: %s",
            event
        )

        notification_id = event["notification_id"]
        retry_count = event["retry_count"]
        user_id = event["user_id"]
        channel = event.get("channel", "SMS")

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
                    "Notification not found: notification_id=%s",
                    notification_id,
                )

                continue

            logger.info(
                "Processing notification_id=%s | retry_count=%s",
                notification.id,
                retry_count,
            )

            try:

                send_sms(notification)

                notification.status = (
                    NotificationStatus.SENT
                )

                db.commit()

                # Increment successful SMS counter
                sms_processed.inc()

                logger.info(
                    "Notification %s marked SENT",
                    notification.id,
                )

            except Exception:

                # Increment failed SMS counter
                sms_failed.inc()

                # logger.exception() automatically includes
                # the exception + full traceback
                logger.exception(
                    "SMS delivery failed for notification_id=%s",
                    notification.id,
                )

                retry_count += 1

                logger.warning(
                    "Publishing retry event | "
                    "notification_id=%s | retry_count=%s",
                    notification.id,
                    retry_count,
                )

                publish_event(
                    topic="notifications.retry",
                    user_id=user_id,
                    notification_id=notification.id,
                    channel=channel,
                    retry_count=retry_count,
                )

                logger.info(
                    "Retry event published | "
                    "notification_id=%s | retry_count=%s",
                    notification.id,
                    retry_count,
                )

        finally:
            db.close()

            logger.debug(
                "Database session closed"
            )


if __name__ == "__main__":
    main()
