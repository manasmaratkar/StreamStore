import json

from confluent_kafka import Consumer

consumer_config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'order-tracker',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(consumer_config)

consumer.subscribe(['orders'])

print(f'Listening for messages on topic "orders"...')

try:
    while True:
        msg = consumer.poll(1.0)  # Wait for a message for up to 1 second

        if msg is None:
            continue  # No message received, continue polling
        if msg.error():
            print(f'❌Consumer error: {msg.error()}')
            continue

        value = msg.value().decode('utf-8')
        order_data = json.loads(value)
        print(f'Received order: {order_data["quantity"]} x {order_data["item"]} from {order_data["user"]}')
except KeyboardInterrupt:
    print('\nStopping consumer...')
finally:
    consumer.close()
