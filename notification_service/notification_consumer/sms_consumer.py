import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone

from kafka import KafkaConsumer
from prometheus_client import Counter, start_http_server

from database import SessionLocal
from kafka_producer import publish_event
from models import Notification, NotificationStatus


# ============================================================
# Logging
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

logger = logging.getLogger("sms-consumer")


# ============================================================
# Retry configuration
# ============================================================

MAX_RETRIES = 3
INITIAL_RETRY_DELAY = 5


# ============================================================
# Prometheus metrics
# ============================================================

sms_processed = Counter(
    "sms_notifications_processed_total",
    "Total number of SMS notifications successfully processed",
)

sms_failed = Counter(
    "sms_notifications_failed_total",
    "Total number of SMS delivery failures",
)

sms_dlq = Counter(
    "sms_notifications_dlq_total",
    "Total number of SMS notifications moved to DLQ",
)


# ============================================================
# Simulated SMS provider
# ============================================================

def send_sms(notification):
    """
    Simulated SMS provider.

    Intentionally fails so we can test:

        PENDING
            ↓
        RETRY_PENDING
            ↓
        retry
            ↓
        RETRY_PENDING
            ↓
        ...
            ↓
        FAILED + DLQ
    """

    logger.info(
        "Sending SMS | notification_id=%s | recipient=%s",
        notification.id,
        notification.recipient,
    )

    time.sleep(2)

    logger.info("SMS Provider: Twilio")
    logger.info("Connecting to SMS provider...")
    logger.info("Preparing SMS request...")
    logger.info("To: %s", notification.recipient)

    # Simulated failure
    raise Exception(
        "Twilio SMS delivery failed "
        "(simulated failure for retry/DLQ testing)"
    )


# ============================================================
# Retry / DLQ handling
# ============================================================

def schedule_retry(db, notification, error):
    """
    Decide whether the notification should be retried
    or moved to the DLQ.

    retry_count represents the number of failed attempts.

    With MAX_RETRIES = 3:

        Attempt 1 fails -> retry_count = 1 -> retry
        Attempt 2 fails -> retry_count = 2 -> retry
        Attempt 3 fails -> retry_count = 3 -> retry
        Attempt 4 fails -> retry_count = 4 -> FAILED + DLQ
    """

    notification.retry_count += 1
    notification.last_error = str(error)

    retry_count = notification.retry_count

    # --------------------------------------------------------
    # Retry
    # --------------------------------------------------------

    if retry_count <= MAX_RETRIES:

        delay_seconds = INITIAL_RETRY_DELAY * (
            2 ** (retry_count - 1)
        )

        notification.status = NotificationStatus.RETRY_PENDING

        notification.next_retry_at = (
            datetime.now(timezone.utc)
            + timedelta(seconds=delay_seconds)
        )

        db.commit()

        logger.warning(
            "SMS retry scheduled | "
            "notification_id=%s | "
            "retry_count=%d/%d | "
            "delay=%ds | "
            "next_retry_at=%s",
            notification.id,
            retry_count,
            MAX_RETRIES,
            delay_seconds,
            notification.next_retry_at,
        )

        return "RETRY"

    # --------------------------------------------------------
    # Retries exhausted
    # --------------------------------------------------------

    notification.status = NotificationStatus.FAILED
    notification.next_retry_at = None

    db.commit()

    logger.error(
        "SMS permanently failed | "
        "notification_id=%s | "
        "retry_count=%d | "
        "moving to DLQ",
        notification.id,
        retry_count,
    )

    # --------------------------------------------------------
    # Publish DLQ event
    # --------------------------------------------------------

    try:

        publish_event(
            topic="notifications.dlq",
            user_id=notification.user_id,
            notification_id=notification.id,
            channel=notification.channel,
            retry_count=retry_count,
        )

        sms_dlq.inc()

        logger.warning(
            "DLQ event published | "
            "notification_id=%s | "
            "channel=%s | "
            "retry_count=%d",
            notification.id,
            notification.channel,
            retry_count,
        )

    except Exception:

        logger.exception(
            "Failed to publish DLQ event | "
            "notification_id=%s",
            notification.id,
        )

    return "FAILED"


# ============================================================
# Main
# ============================================================

def main():

    logger.info("Starting SMS consumer...")

    # --------------------------------------------------------
    # Prometheus
    # --------------------------------------------------------

    start_http_server(9100)

    logger.info(
        "Prometheus metrics server started on port 9100"
    )

    # --------------------------------------------------------
    # Kafka
    # --------------------------------------------------------

    consumer = KafkaConsumer(
        "notifications.sms",

        bootstrap_servers=os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS",
            "kafka:9092",
        ),

        group_id="sms-group",

        auto_offset_reset="latest",

        # IMPORTANT:
        # We manually commit only after DB processing succeeds.
        enable_auto_commit=False,

        value_deserializer=lambda m: json.loads(
            m.decode("utf-8")
        ),
    )

    logger.info(
        "Connected to Kafka topic=notifications.sms"
    )

    # --------------------------------------------------------
    # Consume events
    # --------------------------------------------------------

    for message in consumer:

        event = message.value

        notification_id = event["notification_id"]

        event_retry_count = event.get("retry_count", 0)

        logger.info(
            "Received SMS event | "
            "partition=%s | "
            "offset=%s | "
            "notification_id=%s | "
            "event_retry_count=%s",
            message.partition,
            message.offset,
            notification_id,
            event_retry_count,
        )

        db = SessionLocal()

        try:

            # ------------------------------------------------
            # IMPORTANT:
            #
            # Lock this notification row.
            #
            # This prevents the retry scheduler and SMS
            # consumer from modifying the same notification
            # at the same time.
            # ------------------------------------------------

            notification = (
                db.query(Notification)
                .filter(
                    Notification.id == notification_id
                )
                .with_for_update()
                .first()
            )

            # ------------------------------------------------
            # Notification not found
            # ------------------------------------------------

            if notification is None:

                logger.error(
                    "Notification not found | "
                    "notification_id=%s",
                    notification_id,
                )

                # Nothing useful can be done with this event.
                consumer.commit()

                continue

            logger.info(
                "Processing SMS | "
                "notification_id=%s | "
                "status=%s | "
                "db_retry_count=%s | "
                "event_retry_count=%s",
                notification.id,
                notification.status,
                notification.retry_count,
                event_retry_count,
            )

            # ------------------------------------------------
            # STALE EVENT PROTECTION
            # ------------------------------------------------
            #
            # Example:
            #
            # Kafka has an old retry event:
            #     retry_count = 1
            #
            # DB already says:
            #     retry_count = 2
            #
            # Therefore this Kafka event is stale.
            # Do NOT send another SMS.
            # ------------------------------------------------

            if event_retry_count < notification.retry_count:

                logger.warning(
                    "Ignoring stale SMS event | "
                    "notification_id=%s | "
                    "event_retry_count=%s | "
                    "db_retry_count=%s",
                    notification.id,
                    event_retry_count,
                    notification.retry_count,
                )

                db.rollback()

                consumer.commit()

                continue

            # ------------------------------------------------
            # Already successfully sent
            # ------------------------------------------------

            if notification.status == NotificationStatus.SENT:

                logger.info(
                    "Notification already SENT | "
                    "notification_id=%s",
                    notification.id,
                )

                db.rollback()

                consumer.commit()

                continue

            # ------------------------------------------------
            # Already permanently failed
            # ------------------------------------------------

            if notification.status == NotificationStatus.FAILED:

                logger.info(
                    "Notification already FAILED | "
                    "notification_id=%s",
                    notification.id,
                )

                db.rollback()

                consumer.commit()

                continue

            # ------------------------------------------------
            # Attempt SMS
            # ------------------------------------------------

            try:

                send_sms(notification)

                # --------------------------------------------
                # SUCCESS
                # --------------------------------------------

                notification.status = NotificationStatus.SENT
                notification.next_retry_at = None
                notification.last_error = None

                db.commit()

                sms_processed.inc()

                logger.info(
                    "SMS successfully sent | "
                    "notification_id=%s",
                    notification.id,
                )

                # Kafka offset AFTER DB success
                consumer.commit()

            except Exception as exc:

                # --------------------------------------------
                # FAILURE
                # --------------------------------------------

                sms_failed.inc()

                logger.exception(
                    "SMS delivery failed | "
                    "notification_id=%s",
                    notification.id,
                )

                result = schedule_retry(
                    db=db,
                    notification=notification,
                    error=exc,
                )

                logger.info(
                    "AFTER retry handling | "
                    "notification_id=%s | "
                    "status=%s | "
                    "retry_count=%s | "
                    "next_retry_at=%s",
                    notification.id,
                    notification.status,
                    notification.retry_count,
                    notification.next_retry_at,
                )

                # Kafka offset AFTER DB success
                consumer.commit()

        except Exception:

            db.rollback()

            logger.exception(
                "Unexpected error processing SMS | "
                "notification_id=%s",
                notification_id,
            )

            # IMPORTANT:
            # Do NOT commit the Kafka offset here.
            #
            # Kafka will redeliver the message.

        finally:

            db.close()

            logger.debug(
                "Database session closed | "
                "notification_id=%s",
                notification_id,
            )


# ============================================================
# Entry point
# ============================================================

if __name__ == "__main__":
    main()