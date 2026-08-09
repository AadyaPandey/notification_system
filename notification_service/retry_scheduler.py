import logging
import time
from datetime import datetime, timezone

from prometheus_client import Counter, Gauge, start_http_server
from sqlalchemy import func

from database import SessionLocal
from kafka_producer import publish_event
from models import (
    Notification,
    NotificationChannel,
    NotificationStatus,
)


# ============================================================
# Logging
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

logger = logging.getLogger("retry-scheduler")


# ============================================================
# Scheduler configuration
# ============================================================

POLL_INTERVAL_SECONDS = 1
BATCH_SIZE = 100

METRICS_PORT = 9100

retry_events_published = Counter(
    "notification_retry_events_published_total",
    "Total number of retry events successfully published to Kafka",
    ["channel"],
)

retry_publish_failures = Counter(
    "notification_retry_publish_failures_total",
    "Total number of retry event publish failures",
    ["channel"],
)

notification_status_count = Gauge(
    "notification_status_count",
    "Current number of notifications by channel and status",
    ["channel", "status"],
)

notification_retry_pending_count = Gauge(
    "notification_retry_pending_count",
    "Current number of notifications waiting for a retry",
)

# ============================================================
# Topic mapping
# ============================================================

TOPIC_MAP = {
    "EMAIL": "notifications.email",
    "SMS": "notifications.sms",
    "PUSH": "notifications.push",
}

def refresh_status_metrics(db):
    counts = (
        db.query(
            Notification.channel,
            Notification.status,
            func.count(Notification.id),
        )
        .group_by(
            Notification.channel,
            Notification.status,
        )
        .all()
    )

    observed = {
        (channel.value, status.value): int(count)
        for channel, status, count in counts
    }

    total_retry_pending = 0

    for channel in NotificationChannel:
        for status in NotificationStatus:
            value = observed.get(
                (channel.value, status.value),
                0,
            )

            notification_status_count.labels(
                channel=channel.value,
                status=status.value,
            ).set(value)

            if status == NotificationStatus.RETRY_PENDING:
                total_retry_pending += value

    notification_retry_pending_count.set(
        total_retry_pending
    )

# ============================================================
# Process due retries
# ============================================================

def process_due_notifications():

    db = SessionLocal()

    try:

        now = datetime.now(timezone.utc)

        # ----------------------------------------------------
        # Find notifications that are ready.
        #
        # FOR UPDATE locks the selected rows.
        # ----------------------------------------------------

        notifications = (
            db.query(Notification)
            .filter(
                Notification.status
                == NotificationStatus.RETRY_PENDING,

                Notification.next_retry_at.isnot(None),

                Notification.next_retry_at <= now,
            )
            .order_by(
                Notification.next_retry_at
            )
            .with_for_update(skip_locked=True)
            .limit(BATCH_SIZE)
            .all()
        )

        refresh_status_metrics(db)

        if not notifications:
            return

        logger.info(
            "Found %d notifications ready for retry",
            len(notifications),
        )

        # ----------------------------------------------------
        # Process each notification
        # ----------------------------------------------------

        for notification in notifications:

            try:

                logger.info(
                    "Retrying notification | "
                    "notification_id=%s | "
                    "channel=%s | "
                    "retry_count=%d",
                    notification.id,
                    notification.channel,
                    notification.retry_count,
                )

                # ------------------------------------------------
                # Select Kafka topic
                # ------------------------------------------------

                topic = TOPIC_MAP.get(
                    notification.channel
                )

                # ------------------------------------------------
                # Invalid channel
                # ------------------------------------------------

                if topic is None:

                    logger.error(
                        "Invalid notification channel | "
                        "notification_id=%s | "
                        "channel=%s",
                        notification.id,
                        notification.channel,
                    )

                    notification.status = (
                        NotificationStatus.FAILED
                    )

                    notification.next_retry_at = None

                    db.commit()

                    continue

                # ------------------------------------------------
                # IMPORTANT:
                #
                # Claim the notification BEFORE publishing.
                #
                # The row is already locked by FOR UPDATE.
                # ------------------------------------------------

                notification.status = (
                    NotificationStatus.PENDING
                )

                notification.next_retry_at = None

                # Flush the DB change while keeping the
                # transaction open.
                db.flush()

                logger.info(
                    "Retry claimed | "
                    "notification_id=%s | "
                    "status=PENDING",
                    notification.id,
                )

                # ------------------------------------------------
                # Publish retry event
                # ------------------------------------------------

                publish_event(
                    topic=topic,
                    user_id=notification.user_id,
                    notification_id=notification.id,
                    channel=notification.channel,
                    retry_count=notification.retry_count,
                )

                logger.info(
                    "Retry event published | "
                    "notification_id=%s | "
                    "topic=%s | "
                    "retry_count=%d",
                    notification.id,
                    topic,
                    notification.retry_count,
                )

                # ------------------------------------------------
                # Commit DB state
                # ------------------------------------------------

                db.commit()

                logger.info(
                    "Retry transaction committed | "
                    "notification_id=%s | "
                    "status=PENDING",
                    notification.id,
                )

            except Exception:

                db.rollback()

                logger.exception(
                    "Failed to process retry | "
                    "notification_id=%s",
                    notification.id,
                )

                # Since the transaction rolled back,
                # the notification returns to:
                #
                # RETRY_PENDING
                #
                # and the scheduler can try again later.

    finally:

        db.close()


# ============================================================
# Main scheduler
# ============================================================

def main():

    logger.info(
        "Starting Retry Scheduler..."
    )

    start_http_server(METRICS_PORT)

    logger.info(
        "Prometheus metrics server started on port %s",
        METRICS_PORT,
    )

    while True:

        try:

            process_due_notifications()

        except Exception:

            logger.exception(
                "Unexpected error in Retry Scheduler"
            )

        time.sleep(
            POLL_INTERVAL_SECONDS
        )


# ============================================================
# Entry point
# ============================================================

if __name__ == "__main__":
    main()