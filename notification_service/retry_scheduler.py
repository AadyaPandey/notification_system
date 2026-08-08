import logging
import time
from datetime import datetime, timezone

from database import SessionLocal
from kafka_producer import publish_event
from models import Notification, NotificationStatus


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

logger = logging.getLogger(__name__)


# Scheduler checks the database every 1 second
POLL_INTERVAL_SECONDS = 1

# Maximum number of notifications processed in one scheduler run
BATCH_SIZE = 100


def process_due_notifications():
    """
    Find notifications whose retry time has arrived
    and publish them back to the main email Kafka topic.
    """

    db = SessionLocal()

    try:
        now = datetime.now(timezone.utc)

        notifications = (
            db.query(Notification)
            .filter(
                Notification.status
                == NotificationStatus.RETRY_PENDING,

                Notification.next_retry_at <= now,
            )
            .order_by(Notification.next_retry_at)
            .limit(BATCH_SIZE)
            .all()
        )

        if not notifications:
            return

        logger.info(
            "Found %d notifications ready for retry",
            len(notifications),
        )

        for notification in notifications:

            try:
                logger.info(
                    "Retrying notification %s | retry_count=%d",
                    notification.id,
                    notification.retry_count,
                )

                # Send the notification back to the
                # MAIN email topic.
                publish_event(
                    topic="notifications.email",
                    user_id=notification.user_id,
                    notification_id=notification.id,
                    channel="EMAIL",
                    retry_count=notification.retry_count,
                )

                # Kafka publish succeeded.
                #
                # Change the status back to PENDING so that
                # the Email Consumer can process it normally.
                notification.status = NotificationStatus.PENDING

                # The current retry has been scheduled,
                # so there is no longer a pending retry time.
                notification.next_retry_at = None

                db.commit()

                logger.info(
                    "Notification %s published to "
                    "notifications.email successfully",
                    notification.id,
                )

            except Exception:
                db.rollback()

                logger.exception(
                    "Failed to publish notification %s "
                    "for retry",
                    notification.id,
                )

    finally:
        db.close()


def main():
    logger.info("Starting Retry Scheduler...")

    while True:

        try:
            process_due_notifications()

        except Exception:
            logger.exception(
                "Unexpected error in Retry Scheduler"
            )

        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()