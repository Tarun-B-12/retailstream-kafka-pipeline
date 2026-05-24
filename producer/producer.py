import json
import time
import uuid
import random
from datetime import datetime
from confluent_kafka import Producer
from dotenv import load_dotenv
import os

load_dotenv()

conf = {
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': os.getenv('KAFKA_API_KEY'),
    'sasl.password': os.getenv('KAFKA_API_SECRET'),
}
# Validate env variables are loaded
required = ['KAFKA_BOOTSTRAP_SERVERS', 'KAFKA_API_KEY', 'KAFKA_API_SECRET', 'KAFKA_TOPIC']
missing = [v for v in required if not os.getenv(v)]
if missing:
    raise EnvironmentError(f"Missing required environment variables: {missing}")
producer = Producer(conf)
topic = os.getenv('KAFKA_TOPIC')

STORES = [f"STORE_{str(i).zfill(3)}" for i in range(1, 21)]
CATEGORIES = ["Electronics", "Apparel", "Food", "Home", "Sports"]
PAYMENTS = ["credit_card", "debit_card", "cash", "mobile_pay"]
SEGMENTS = ["premium", "standard", "budget"]

def generate_event():
    quantity = random.randint(1, 5)
    unit_price = round(random.uniform(5.0, 499.99), 2)
    return {
        "event_id": str(uuid.uuid4()),
        "timestamp": datetime.utcnow().isoformat(),
        "store_id": random.choice(STORES),
        "product_category": random.choice(CATEGORIES),
        "quantity": quantity,
        "unit_price": unit_price,
        "total_amount": round(quantity * unit_price, 2),
        "payment_method": random.choice(PAYMENTS),
        "customer_segment": random.choice(SEGMENTS),
    }

def delivery_report(err, msg):
    if err:
        print(f"Delivery failed: {err}")
    else:
        print(f"Event sent → {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")

print("Producer started. Sending 1 event per second. Ctrl+C to stop.\n")

try:
    while True:
        event = generate_event()
        producer.produce(
            topic,
            key=event["store_id"],
            value=json.dumps(event),
            callback=delivery_report
        )
        producer.poll(0)
        time.sleep(1)
except KeyboardInterrupt:
    print("\nStopping producer...")
finally:
    producer.flush()
    print("Done.")