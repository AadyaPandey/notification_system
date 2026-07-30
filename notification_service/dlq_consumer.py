import json
import os 
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "notifications.dlq",
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
    auto_offset_reset="earliest",
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

print("DLQ Consumer Started...")

for message in consumer:
    print("\n========== DEAD LETTER ==========")
    print(message.value)
    print("=================================\n")