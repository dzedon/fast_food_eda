import json
import time
import logging


from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO)

ORDER_KAFKA_TOPIC = "orders_details"
ORDER_LIMIT = 15

producer = KafkaProducer(bootstrap_servers="localhost:29092")

logging.info("Going to generating order after 10 seconds")
logging.info("Will generate unique order every 10 seconds")

for i in range(1, ORDER_LIMIT):
    data = {
        "order_id": i,
        "user_id": f"Daniel{i}",
        "total_cost": i * 2,
        "items": "burger",
    }

    producer.send(ORDER_KAFKA_TOPIC, json.dumps(data).encode("utf-8"))

    logging.info(f"Generated order: {i+1}")
    time.sleep(5)
