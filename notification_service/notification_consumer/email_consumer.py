import json
import logging
import os
import smtplib
from datetime import datetime, timedelta, timezone

from prometheus_client import Counter, start_http_server
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from kafka import KafkaConsumer

from database import SessionLocal
from kafka_producer import publish_event
from models import Notification, NotificationStatus


# --------------------------------------------------
# Logging configuration
# --------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

logger = logging.getLogger(__name__)


# --------------------------------------------------
# Email configuration
# --------------------------------------------------

EMAIL = os.getenv("EMAIL_ADDRESS")
APP_PASSWORD = os.getenv("EMAIL_APP_PASSWORD")


# --------------------------------------------------
# Retry configuration
# --------------------------------------------------

MAX_RETRIES = 3
INITIAL_RETRY_DELAY = 5


# --------------------------------------------------
# Prometheus metrics
# --------------------------------------------------

emails_processed = Counter(
    "email_notifications_processed_total",
    "Total number of email notifications successfully processed",
)

emails_failed = Counter(
    "email_notifications_failed_total",
    "Total number of email notifications that failed",
)

emails_dlq = Counter(
    "email_notifications_dlq_total",
    "Total number of email notifications moved to DLQ",
)

emails_retries_scheduled = Counter(
    "email_notification_retries_scheduled_total",
    "Total number of email retries scheduled",
)


# --------------------------------------------------
# Send email
# --------------------------------------------------

def send_email(notification):

    if not EMAIL:
        raise ValueError("EMAIL_ADDRESS not set")

    if not APP_PASSWORD:
        raise ValueError("EMAIL_APP_PASSWORD not set")

    logger.info(
        "Sending email to %s",
        notification.recipient,
    )

    msg = MIMEMultipart("alternative")

    msg["Subject"] = notification.subject
    msg["From"] = EMAIL
    msg["To"] = notification.recipient

    html = f"""
    <tr>
      <td align="center">

        <table width="600" cellpadding="0" cellspacing="0"
               style="background:#ffffff;border-radius:12px;overflow:hidden;
                      box-shadow:0 4px 12px rgba(0,0,0,0.08);">

          <!-- Header -->
          <tr>
            <td align="center"
                style="background:#2563eb;padding:24px;color:white;">

              <h1 style="margin:0;font-size:28px;">
                FundWise
              </h1>

              <p style="margin-top:8px;font-size:15px;">
                Grant Application Review
              </p>

            </td>
          </tr>

          <!-- Body -->
          <tr>
            <td style="padding:35px;color:#333;
                       line-height:1.7;font-size:16px;">

              {notification.message.replace(chr(10), "<br>")}

            </td>
          </tr>

          <!-- Divider -->
          <tr>
            <td>
              <hr style="border:none;border-top:1px solid #e5e7eb;">
            </td>
          </tr>

          <!-- Footer -->
          <tr>
            <td align="center"
                style="padding:20px;color:#6b7280;font-size:13px;">

              This email was generated automatically by
              <strong>FundWise</strong>.<br>

              Please do not reply to this email.

            </td>
          </tr>

        </table>

      </td>
    </tr>
    """

    msg.attach(
        MIMEText(html, "html")
    )

    with smtplib.SMTP(
        "smtp.gmail.com",
        587,
    ) as smtp:

        smtp.ehlo()
        smtp.starttls()
        smtp.ehlo()

        smtp.login(
            EMAIL,
            APP_PASSWORD,
        )

        smtp.sendmail(
            EMAIL,
            notification.recipient,
            msg.as_string(),
        )

    logger.info(
        "Email sent successfully to %s",
        notification.recipient,
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
    After that -> FAILED + DLQ
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

        notification.status = (
            NotificationStatus.RETRY_PENDING
        )

        notification.next_retry_at = (
            datetime.now(timezone.utc)
            + timedelta(seconds=delay_seconds)
        )

        db.commit()

        emails_retries_scheduled.inc()

        logger.info(
            "Notification %s scheduled for retry "
            "%d/%d in %d seconds",
            notification.id,
            notification.retry_count,
            MAX_RETRIES,
            delay_seconds,
        )

    # --------------------------------------------------
    # Maximum retries reached
    # --------------------------------------------------

    else:

        notification.status = (
            NotificationStatus.FAILED
        )

        notification.next_retry_at = None

        # Save FAILED state in PostgreSQL
        db.commit()

        logger.error(
            "Notification %s permanently failed "
            "after %d retries",
            notification.id,
            MAX_RETRIES,
        )

        # --------------------------------------------------
        # Publish failed notification to DLQ
        # --------------------------------------------------

        try:

            publish_event(
                topic="notifications.dlq",
                user_id=notification.user_id,
                notification_id=notification.id,
                channel="EMAIL",
                retry_count=notification.retry_count,
            )

            emails_dlq.inc()

            logger.warning(
                "Notification %s moved to DLQ | "
                "channel=EMAIL | retry_count=%d",
                notification.id,
                notification.retry_count,
            )

        except Exception:

            logger.exception(
                "Failed to publish notification %s "
                "to DLQ",
                notification.id,
            )


# --------------------------------------------------
# Main consumer
# --------------------------------------------------

def main() -> None:

    logger.info(
        "Starting Email Consumer..."
    )

    # --------------------------------------------------
    # Prometheus
    # --------------------------------------------------

    start_http_server(9100)

    logger.info(
        "Prometheus metrics server started "
        "on port 9100"
    )

    # --------------------------------------------------
    # Kafka consumer
    # --------------------------------------------------

    consumer = KafkaConsumer(
        "notifications.email",
        bootstrap_servers=os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS"
        ),
        group_id="email-group",
        auto_offset_reset="latest",
        value_deserializer=lambda m: json.loads(
            m.decode("utf-8")
        ),
    )

    logger.info(
        "Waiting for Kafka messages..."
    )

    # --------------------------------------------------
    # Consume messages
    # --------------------------------------------------

    for message in consumer:

        logger.info(
            "Received Kafka message | "
            "partition=%s offset=%s",
            message.partition,
            message.offset,
        )

        event = message.value

        logger.info(
            "Event: %s",
            event,
        )

        notification_id = event[
            "notification_id"
        ]

        db = SessionLocal()

        try:

            notification = (
                db.query(Notification)
                .filter(
                    Notification.id
                    == notification_id
                )
                .first()
            )

            # --------------------------------------------------
            # Notification doesn't exist
            # --------------------------------------------------

            if notification is None:

                logger.warning(
                    "Notification %s not found",
                    notification_id,
                )

                continue

            logger.info(
                "Processing notification %s | "
                "retry_count=%d",
                notification.id,
                notification.retry_count,
            )

            # --------------------------------------------------
            # Already sent
            # --------------------------------------------------

            if (
                notification.status
                == NotificationStatus.SENT
            ):

                logger.info(
                    "Notification %s already sent. "
                    "Skipping duplicate delivery.",
                    notification.id,
                )

                continue

            # --------------------------------------------------
            # Already permanently failed
            # --------------------------------------------------

            if (
                notification.status
                == NotificationStatus.FAILED
            ):

                logger.info(
                    "Notification %s already FAILED. "
                    "Skipping.",
                    notification.id,
                )

                continue

            # --------------------------------------------------
            # Attempt email delivery
            # --------------------------------------------------

            try:

                send_email(notification)

                # --------------------------------------------------
                # SUCCESS
                # --------------------------------------------------

                notification.status = (
                    NotificationStatus.SENT
                )

                notification.next_retry_at = None
                notification.last_error = None

                db.commit()

                emails_processed.inc()

                logger.info(
                    "Notification %s marked as SENT",
                    notification.id,
                )

            except Exception as exc:

                # --------------------------------------------------
                # FAILURE
                # --------------------------------------------------

                emails_failed.inc()

                logger.exception(
                    "Failed to send email for "
                    "notification %s",
                    notification.id,
                )

                schedule_retry(
                    db=db,
                    notification=notification,
                    error=exc,
                )

        except Exception:

            db.rollback()

            logger.exception(
                "Unexpected error while processing "
                "email notification"
            )

        finally:

            db.close()

            logger.info(
                "Database session closed"
            )


# --------------------------------------------------
# Entry point
# --------------------------------------------------

if __name__ == "__main__":
    main()