import json
import logging
import os

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

logger = logging.getLogger("dlq-consumer")


# --------------------------------------------------
# Prometheus metrics
# --------------------------------------------------

dlq_processed = Counter(
    "notification_dlq_processed_total",
    "Total number of DLQ notifications successfully processed",
    ["channel"],
)

dlq_failed = Counter(
    "notification_dlq_failed_total",
    "Total number of DLQ notifications that failed to process",
    ["channel"],
)


# --------------------------------------------------
# Main consumer
# --------------------------------------------------

def main() -> None:

    logger.info("Starting DLQ consumer...")

    # Start Prometheus metrics server
    start_http_server(9100)

    logger.info(
        "Prometheus metrics server started on port 9100"
    )

    consumer = KafkaConsumer(
        "notifications.dlq",
        bootstrap_servers=os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS"
        ),
        group_id="dlq-group",
        auto_offset_reset="latest",
        value_deserializer=lambda m: json.loads(
            m.decode("utf-8")
        ),
    )

    logger.info(
        "Connected to Kafka topic=notifications.dlq"
    )

    for message in consumer:

        event = message.value

        logger.info(
            "Received DLQ event | event=%s",
            event,
        )

        notification_id = event["notification_id"]
        retry_count = event["retry_count"]
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
                    "Notification not found | "
                    "notification_id=%s",
                    notification_id,
                )

                # This is a processing failure because
                # the DLQ event could not be handled.
                dlq_failed.labels(
                    channel=channel
                ).inc()

                continue

            # --------------------------------------------------
            # Mark notification as FAILED
            # --------------------------------------------------

            notification.status = NotificationStatus.FAILED

            db.commit()

            # Count successfully processed DLQ event
            dlq_processed.labels(
                channel=channel
            ).inc()

            logger.warning(
                "Notification moved to FAILED via DLQ | "
                "notification_id=%s | "
                "channel=%s | "
                "recipient=%s | "
                "retry_attempts=%s",
                notification.id,
                channel,
                notification.recipient,
                retry_count,
            )

        except Exception:

            # Count failed DLQ processing
            dlq_failed.labels(
                channel=channel
            ).inc()

            # Log full traceback if database processing fails
            logger.exception(
                "Failed to process DLQ event | "
                "notification_id=%s",
                notification_id,
            )

            db.rollback()

        finally:

            db.close()

            logger.debug(
                "Database session closed | "
                "notification_id=%s",
                notification_id,
            )


if __name__ == "__main__":
    main()
