import csv
import json
import logging
import time
from kafka import KafkaProducer
import pandas as pd

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Kafka configuration
KAFKA_BROKER = 'localhost:9092'
TOPIC_NAME = 'sensor_data'

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Define CSV file path (modify as needed)
CSV_FILE_PATH = 'sensor_readings.csv'

def generate_messages(csv_file):
    """Reads the CSV file and sends messages to Kafka."""
    try:
        df = pd.read_csv(csv_file)
        required_columns = {'sensor_id', 'temperature', 'humidity', 'timestamp'}
        
        if not required_columns.issubset(df.columns):
            raise ValueError(f"CSV file must contain columns: {required_columns}")
        
        for _, row in df.iterrows():
            message = {
                'sensor_id': row['sensor_id'],
                'temperature': row['temperature'],
                'humidity': row['humidity'],
                'timestamp': row['timestamp']
            }
            producer.send(TOPIC_NAME, value=message)
            logger.info(f"Sent message: {message}")
            time.sleep(1)  # Simulate real-time streaming
    except Exception as e:
        logger.error(f"Error processing CSV file: {e}")

if __name__ == "__main__":
    generate_messages(CSV_FILE_PATH)
    producer.close()