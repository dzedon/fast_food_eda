import json
import logging

from kafka import KafkaConsumer
from kafka import KafkaProducer


logging.basicConfig(level=logging.INFO)

DLQ_TOPIC = "orders_details_dlq"
GROUP_ID = "MY_GROUP"

consumer = KafkaConsumer(
    DLQ_TOPIC,
    group_id=GROUP_ID,
    bootstrap_servers="localhost:29092",
)

for message in consumer:
    consumed_message = json.loads(message.value.decode())
    logging.info(consumed_message)
