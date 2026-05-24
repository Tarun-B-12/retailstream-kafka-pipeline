import os
from dotenv import load_dotenv

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
KAFKA_API_KEY = os.getenv('KAFKA_API_KEY')
KAFKA_API_SECRET = os.getenv('KAFKA_API_SECRET')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'retail-transactions')

HIGH_VALUE_THRESHOLD = 1000
OUTLIER_THRESHOLD = 5000
CONSUMER_GROUP_ID = 'retailstream-consumer-group-v2'
DB_PATH = 'storage/retailstream.duckdb'
DASHBOARD_REFRESH_SECONDS = 5