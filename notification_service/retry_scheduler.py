import logging
import time
from datetime import datetime, timezone

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

logger = logging.getLogger("retry-scheduler")


# ============================================================
# Scheduler configuration
# ============================================================

POLL_INTERVAL_SECONDS = 1
BATCH_SIZE = 100


# ============================================================
# Topic mapping
# ============================================================

TOPIC_MAP = {
    "EMAIL": "notifications.email",
    "SMS": "notifications.sms",
    "PUSH": "notifications.push",
}


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