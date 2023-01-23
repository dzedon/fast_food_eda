import json
import logging

from kafka import KafkaConsumer
from kafka import KafkaProducer


logging.basicConfig(level=logging.INFO)

ORDER_CONFIRM_KAFKA_TOPIC = "orders_confirmed"

consumer = KafkaConsumer(
    ORDER_CONFIRM_KAFKA_TOPIC,
    bootstrap_servers="localhost:29092",
)

total_orders = 0
total_revenue = 0

logging.info("Analytics service started..")

while True:
    for message in consumer:
        logging.info("Updating analytics..")
        consumed_message = json.loads(message.value.decode())
        total_cost = float(consumed_message.get("total_cost"))
        total_orders += 1
        total_revenue += total_cost

        logging.info(f"Orders so far today {total_orders}")
        logging.info(f"Revenue so far today {total_revenue}")
