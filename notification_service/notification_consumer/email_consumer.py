import json
import logging
import os
import smtplib
from prometheus_client import Counter, start_http_server
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from kafka import KafkaConsumer

from database import SessionLocal
from kafka_producer import publish_event
from models import Notification, NotificationStatus


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

logger = logging.getLogger(__name__)

EMAIL = os.getenv("EMAIL_ADDRESS")
APP_PASSWORD = os.getenv("EMAIL_APP_PASSWORD")

emails_processed = Counter(
    "email_notifications_processed_total",
    "Total number of email notifications successfully processed",
)

emails_failed = Counter(
    "email_notifications_failed_total",
    "Total number of email notifications that failed",
)


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
<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <title>FundWise Notification</title>
</head>
<body style="margin:0;padding:0;background-color:#f4f7fb;font-family:Arial,Helvetica,sans-serif;">

  <table width="100%" cellpadding="0" cellspacing="0" style="padding:40px 0;">
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
            <td style="padding:35px;color:#333;line-height:1.7;font-size:16px;">
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
  </table>

</body>
</html>
"""

    msg.attach(MIMEText(html, "html"))

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

    logger.info(
        "Email sent successfully to %s",
        notification.recipient,
    )


def main() -> None:
    logger.info("Starting Email Consumer...")
    start_http_server(9100)
    logger.info("Prometheus metrics server started on port 9100")

    consumer = KafkaConsumer(
        "notifications.email",
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
        group_id="email-group",
        auto_offset_reset="latest",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )

    logger.info("Waiting for Kafka messages...")

    for message in consumer:
        logger.info(
            "Received Kafka message | partition=%s offset=%s",
            message.partition,
            message.offset,
        )

        event = message.value
        logger.info("Event: %s", event)

        notification_id = event["notification_id"]
        retry_count = event["retry_count"]
        user_id = event["user_id"]
        channel = event.get("channel", "EMAIL")

        db = SessionLocal()

        try:
            notification = (
                db.query(Notification)
                .filter(Notification.id == notification_id)
                .first()
            )

            if notification is None:
                logger.warning(
                    "Notification %s not found",
                    notification_id,
                )
                continue

            if notification.status == NotificationStatus.SENT:
                logger.info(
                    "Notification %s already sent. Skipping duplicate delivery.",
                    notification.id,
                )
                continue

            try:
                send_email(notification)

                notification.status = NotificationStatus.SENT
                db.commit()

                emails_processed.inc()

                logger.info(
                    "Notification %s marked as SENT",
                    notification.id,
                )

            except Exception:
                emails_failed.inc()
                logger.exception(
                    "Failed to send email for notification %s",
                    notification.id,
                )

                retry_count += 1

                publish_event(
                    topic="notifications.retry",
                    user_id=user_id,
                    notification_id=notification.id,
                    channel=channel,
                    retry_count=retry_count,
                )

                logger.info(
                    "Published retry event for notification %s (retry_count=%d)",
                    notification.id,
                    retry_count,
                )

        finally:
            db.close()
            logger.info("Database session closed")


if __name__ == "__main__":
    main()