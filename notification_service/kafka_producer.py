import json
import os
import logging

from kafka import KafkaProducer

from dotenv import load_dotenv


load_dotenv()

logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "kafka:9092"
)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda m: json.dumps(m).encode("utf-8"),
    acks="all",
    retries=5,
)


def publish_event(
    topic: str,
    user_id,
    notification_id,
    channel: str = None,
    retry_count: int = 0,
) -> None:
    """
    Publish a notification event to the given Kafka topic.

    Args:
        topic: Kafka topic name (e.g. notifications.email)
        user_id: ID of the user the notification belongs to
        notification_id: ID of the notification record
        channel: Delivery channel (EMAIL / SMS / PUSH)
        retry_count: Number of retries attempted so far
    """
    event = {
        "user_id": str(user_id),
        "notification_id": str(notification_id),
        "channel": channel,
        "retry_count": retry_count,
    }

    future = producer.send(topic, value=event)

    try:
        record_metadata = future.get(timeout=10)
        logger.info(
            "Published event to topic=%s partition=%s offset=%s",
            record_metadata.topic,
            record_metadata.partition,
            record_metadata.offset,
        )
    except Exception as exc:
        logger.error("Failed to publish event to topic=%s: %s", topic, exc)
        raise

