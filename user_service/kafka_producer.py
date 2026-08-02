import json
import logging
import os

from dotenv import load_dotenv
from kafka import KafkaProducer

load_dotenv()
print("USER SERVICE KAFKA PRODUCER LOADED")
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

producer = KafkaProducer(
    bootstrap_servers=os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        "kafka:9092",
    ),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)


def publish_user_event(event: dict):
    try:
        logger.info("Publishing event: %s", event)

        future = producer.send(
            "user-events",
            value=event,
        )

        metadata = future.get(timeout=10)

        logger.info(
            "Event published successfully. Topic=%s Partition=%s Offset=%s",
            metadata.topic,
            metadata.partition,
            metadata.offset,
        )

    except Exception:
        logger.exception("Failed to publish user event")
        raise