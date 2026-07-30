from kafka import KafkaProducer
import json
import os 

producer = KafkaProducer(
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)


def publish_event(topic, notification_id, retry_count=0):
    """
    Publish a notification event to a Kafka topic.
    """

    event = {
        "notification_id": str(notification_id),
        "retry_count": retry_count
    }

    producer.send(topic, event)
    producer.flush()