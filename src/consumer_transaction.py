import json
import logging

from kafka import KafkaConsumer
from kafka import KafkaProducer
from kafka import TopicPartition
from kafka import OffsetAndMetadata


logging.basicConfig(level=logging.INFO)

ORDER_KAFKA_TOPIC = "orders_details"
ORDER_CONFIRM_KAFKA_TOPIC = "orders_confirmed"
DLQ_TOPIC = "orders_details_dlq"
GROUP_ID = "MY_GROUP"

consumer = KafkaConsumer(
    enable_auto_commit=False,
    group_id=GROUP_ID,
    bootstrap_servers="localhost:29092",
)

consumer.subscribe([ORDER_KAFKA_TOPIC])

producer = KafkaProducer(bootstrap_servers="localhost:29092")

logging.info("Start listening...")

for message in consumer:
    try:
        logging.info("Ongoing transaction..")
        consumed_message = json.loads(message.value.decode())

        user_id = consumed_message.get("user_id")
        total_cost = consumed_message.get("total_cost")
        order_id = consumed_message.get('order_id')

        # TEMP
        logging.info(f"offset: {message.offset}")

        if int(order_id) % 2 == 0:
            raise Exception
        # /TEMP

        data = {
            "customer_id": user_id,
            "email": f"{user_id}@test.com",
            "total_cost": total_cost,
        }

        producer.send(ORDER_CONFIRM_KAFKA_TOPIC, json.dumps(data).encode("utf-8"))

        topic_partition = TopicPartition(message.topic, message.partition)
        offset = OffsetAndMetadata(message.offset + 1, message.timestamp)

        consumer.commit({topic_partition: offset})
        # consumer.commit()
        logging.info("Success transaction..")

    except Exception:
        logging.exception("Couldn't process message.")
        consumed_message = json.loads(message.value.decode())
        # consumed_message['order_id'] += 1
        producer.send(DLQ_TOPIC, json.dumps(consumed_message).encode("utf-8"))



