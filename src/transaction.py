import json
import logging

from kafka import KafkaConsumer
from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO)

ORDER_KAFKA_TOPIC = "orders_details"
ORDER_CONFIRM_KAFKA_TOPIC = "orders_confirmed"

consumer = KafkaConsumer(
    ORDER_KAFKA_TOPIC,
    bootstrap_servers="localhost:29092",
)

producer = KafkaProducer(bootstrap_servers="localhost:29092")

logging.info("Start listening..")
while True:
    for message in consumer:
        logging.info("Ongoing transaction..")
        consumed_message = json.loads(message.value.decode())
        logging.info(f"Consumed message: {consumed_message}")

        user_id = consumed_message.get("user_id")
        total_cost = consumed_message.get("total_cost")

        data = {
            "customer_id": user_id,
            "email": f"{user_id}@test.com",
            "total_cost": total_cost,
        }

        logging.info("Success transaction..")

        producer.send(ORDER_CONFIRM_KAFKA_TOPIC, json.dumps(data).encode("utf-8"))
