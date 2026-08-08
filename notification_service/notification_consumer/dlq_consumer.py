import json
import logging
import os

from prometheus_client import Counter, start_http_server
from kafka import KafkaConsumer

from database import SessionLocal
from models import Notification, NotificationStatus


# ============================================================
# Logging
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

logger = logging.getLogger("dlq-consumer")


# ============================================================
# Prometheus metrics
# ============================================================

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


# ============================================================
# Main consumer
# ============================================================

def main() -> None:

    logger.info(
        "Starting DLQ consumer..."
    )

    # --------------------------------------------------------
    # Prometheus
    # --------------------------------------------------------

    start_http_server(9100)

    logger.info(
        "Prometheus metrics server started on port 9100"
    )

    # --------------------------------------------------------
    # Kafka consumer
    # --------------------------------------------------------

    consumer = KafkaConsumer(
        "notifications.dlq",

        bootstrap_servers=os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS",
            "kafka:9092",
        ),

        group_id="dlq-group",

        auto_offset_reset="latest",

        # IMPORTANT:
        # Commit only after DB processing succeeds.
        enable_auto_commit=False,

        value_deserializer=lambda m: json.loads(
            m.decode("utf-8")
        ),
    )

    logger.info(
        "Connected to Kafka topic=notifications.dlq"
    )

    # --------------------------------------------------------
    # Consume DLQ events
    # --------------------------------------------------------

    for message in consumer:

        event = message.value

        notification_id = event["notification_id"]

        retry_count = event.get(
            "retry_count",
            0,
        )

        channel = event.get(
            "channel",
            "UNKNOWN",
        )

        logger.warning(
            "Received DLQ event | "
            "partition=%s | "
            "offset=%s | "
            "notification_id=%s | "
            "retry_count=%s",
            message.partition,
            message.offset,
            notification_id,
            retry_count,
        )

        db = SessionLocal()

        try:

            # ------------------------------------------------
            # Find notification
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
            # Notification doesn't exist
            # ------------------------------------------------

            if notification is None:

                logger.error(
                    "Notification not found | "
                    "notification_id=%s",
                    notification_id,
                )

                dlq_failed.labels(
                    channel=channel
                ).inc()

                # The message cannot be processed.
                # Commit so it does not loop forever.
                consumer.commit()

                continue

            # ------------------------------------------------
            # Already FAILED
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
            # Mark permanently failed
            # ------------------------------------------------

            notification.status = (
                NotificationStatus.FAILED
            )

            notification.next_retry_at = None

            # Keep last_error as it is.
            # It contains the actual provider failure.

            db.commit()

            dlq_processed.labels(
                channel=channel
            ).inc()

            logger.error(
                "Notification permanently FAILED via DLQ | "
                "notification_id=%s | "
                "channel=%s | "
                "recipient=%s | "
                "retry_count=%s",
                notification.id,
                channel,
                notification.recipient,
                retry_count,
            )

            # Kafka offset only after DB commit.
            consumer.commit()

        except Exception:

            db.rollback()

            dlq_failed.labels(
                channel=channel
            ).inc()

            logger.exception(
                "Failed to process DLQ event | "
                "notification_id=%s",
                notification_id,
            )

            # DO NOT commit Kafka offset.
            #
            # Kafka will redeliver the DLQ event.

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