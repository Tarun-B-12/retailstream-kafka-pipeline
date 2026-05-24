import json
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'storage'))

from transformer import transform_event
from anomaly_detector import detect_anomalies
from db import initialize_db, insert_event

from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv

load_dotenv()

conf = {
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': os.getenv('KAFKA_API_KEY'),
    'sasl.password': os.getenv('KAFKA_API_SECRET'),
    'group.id': 'retailstream-consumer-group-v2',
    'auto.offset.reset': 'latest',
}

initialize_db()

consumer = Consumer(conf)
topic = os.getenv('KAFKA_TOPIC')
consumer.subscribe([topic])

print(f"Consumer started. Listening to '{topic}'...\n")

try:
    while True:
        msg = consumer.poll(timeout=1.0)

        if msg is None:
            continue

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(f"Error: {msg.error()}")
            continue

        event = json.loads(msg.value().decode('utf-8'))
        event = transform_event(event)
        event = detect_anomalies(event)
        insert_event(event)

        status = "ANOMALY" if event['has_anomaly'] else "OK"
        print(f"[{status}] {event['timestamp']} | "
              f"{event['store_id']} | "
              f"{event['product_category']} | "
              f"${event['total_amount']} | "
              f"High Value: {event['is_high_value']}")

        if event['has_anomaly']:
            print(f"  ⚠️  {event['anomaly_flags']}")

except KeyboardInterrupt:
    print("\nStopping consumer...")
finally:
    consumer.close()
    print("Done.")