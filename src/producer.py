import json
import time
import logging

from kafka import KafkaProducer
from kafka.errors import KafkaError

logging.basicConfig(level=logging.INFO)

ORDER_KAFKA_TOPIC = "orders_details"
GROUP_ID = 'MY_GROUP'
ORDER_LIMIT = 8
SLEEP_TIMEOUT = 2

producer = KafkaProducer(
    bootstrap_servers="localhost:29092",
    linger_ms=5000,
    acks='all',
    retries=2,
)


def on_success(record):
    """***."""
    logging.info(f"partition: {record.partition}")
    logging.info(f"topic: {record.topic}")
    logging.info(f"offset: {record.offset}")
    logging.info("Success.")


def on_error(my_error):
    """***."""
    logging.info("ERROR.")
    logging.info(my_error)
    producer.send(ORDER_KAFKA_DLQ_TOPIC, json.dumps('data').encode("utf-8"))
    raise Exception(my_error)


logging.info(f"Will generate unique order every {SLEEP_TIMEOUT*2} seconds")


def generate_order():
    """***."""
    for i in range(0, ORDER_LIMIT):
        time.sleep(SLEEP_TIMEOUT)
        data = {
            "order_id": i,
            "user_id": f"Daniel{i}",
            "total_cost": i * 2,
            "items": "burger",
        }
        try:
            producer \
                .send(ORDER_KAFKA_TOPIC, json.dumps(data).encode("utf-8")) \
                .add_callback(on_success) \
                .add_errback(on_error)

            producer.flush()

            logging.info(f"Generated order: {i+1}")


        except Exception:
            logging.Exception("Error")

        time.sleep(SLEEP_TIMEOUT)

    logging.info("QUITTING.")


def generate_order_by_input():
    """***."""
    i = 0
    while True:
        try:
            # user_name = input('Whats your name?\n')
            order_id = input('order id?\n')
            i += 1

            data = {
                "order_id": order_id,
                "user_id": 'dan',
                "total_cost": 0,
                "items": "burger",
            }

            producer\
                .send(ORDER_KAFKA_TOPIC, json.dumps(data).encode("utf-8"))\
                .add_callback(on_success)\
                .add_errback(on_error)

            producer.flush()

            logging.info(f"Generated orders: {i}")
            time.sleep(SLEEP_TIMEOUT)

        except Exception:
            logging.info("Something happened")


if __name__ == "__main__":
    generate_order_by_input()
