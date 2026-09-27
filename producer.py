import json
import uuid

from confluent_kafka import Producer


producer_config = {
    'bootstrap.servers': 'localhost:9092'}

producer = Producer(producer_config)

def delivery_report(err, msg):
    if err is not None:
        print(f'❌Delivery failed for record {msg.key()}: {err}')
    else:
        # print(f'✅Record {msg.key().decode("utf-8")} successfully produced to {msg.topic().decode("utf-8")} [{msg.partition()}] at offset {msg.offset().decode("utf-8")}')
        print(f'✅Delivered {msg.value().decode("utf-8")}')
        print(f'✅Delivered to topic: {msg.topic()} partition: {msg.partition()} offset: {msg.offset()}')

order = {
    'order_id': str(uuid.uuid4()),
    'user': 'Nived',
    'item': 'Yogurt',
    'quantity': 10
}

value = json.dumps(order).encode('utf-8')

producer.produce(
    topic='orders', 
    value=value,
    callback=delivery_report 
)
producer.flush()