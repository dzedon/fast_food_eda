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

email_sent_so_far = set()
logging.info("Email service started..")

while True:
    for message in consumer:
        consumed_message = json.loads(message.value.decode())
        consumer_email = consumed_message.get("email")

        logging.info(f"Sending email to: {consumer_email}")

        email_sent_so_far.add(consumer_email)
        logging.info(f"Unique emails sent so far: {len(email_sent_so_far)}")
