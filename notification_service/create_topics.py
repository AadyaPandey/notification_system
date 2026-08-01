import os
import time
import logging

from dotenv import load_dotenv
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)

load_dotenv()

BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "kafka:9092"
)

TOPICS = {
    "notifications.email": 4,
    "notifications.sms": 4,
    "notifications.push": 4,
    "notifications.retry": 2,
    "notifications.dlq": 1,
}


def wait_for_kafka(admin_client, timeout: int = 60) -> None:
    """Wait until Kafka broker is reachable or raise after timeout."""
    deadline = time.time() + timeout

    while time.time() < deadline:
        try:
            admin_client.list_topics()
            logger.info("Kafka is ready")
            return
        except Exception as exc:  # noqa: BLE001 - Kafka not ready yet
            logger.warning("Kafka not ready yet: %s", exc)
            time.sleep(3)

    raise RuntimeError("Kafka did not become ready in time")


def main() -> None:
    admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)

    wait_for_kafka(admin)

    existing_topics = set(admin.list_topics())

    for name, partitions in TOPICS.items():
        if name in existing_topics:
            logger.info("Topic %s already exists, skipping", name)
            continue

        topic = NewTopic(
            name=name,
            num_partitions=partitions,
            replication_factor=1
        )

        try:
            admin.create_topics(
                new_topics=[topic],
                validate_only=False
            )
            logger.info("Created topic %s", name)
        except TopicAlreadyExistsError:
            logger.info("Topic %s already exists, skipping", name)

    admin.close()

    logger.info("Topic creation finished")


if __name__ == "__main__":
    main()

