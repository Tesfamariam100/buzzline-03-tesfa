"""
csv_consumer_tesfa.py

Consume json messages from a Kafka topic and process them with real-time analytics.

Example Kafka message format:
{"timestamp": "2025-01-11T18:15:00Z", "temperature": 225.0}

"""

#####################################
# Import Modules
#####################################

import os
import json
from collections import deque
from dotenv import load_dotenv
from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger
import pandas as pd

#####################################
# Load Environment Variables
#####################################

load_dotenv()

def get_kafka_topic() -> str:
    topic = os.getenv("SMOKER_TOPIC", "unknown_topic")
    logger.info(f"Kafka topic: {topic}")
    return topic

def get_kafka_consumer_group_id() -> str:
    group_id = os.getenv("SMOKER_CONSUMER_GROUP_ID", "default_group")
    logger.info(f"Kafka consumer group id: {group_id}")
    return group_id

def get_rolling_window_size() -> int:
    window_size = int(os.getenv("SMOKER_ROLLING_WINDOW_SIZE", 5))
    logger.info(f"Rolling window size: {window_size}")
    return window_size

#####################################
# Define a function for real-time analytics
#####################################

def detect_alert(rolling_window: deque) -> bool:
    """Detects specific patterns in messages and triggers an alert."""
    if len(rolling_window) < get_rolling_window_size():
        return False
    temp_range = max(rolling_window) - min(rolling_window)
    alert_triggered = temp_range <= 0.2  # Example threshold
    if alert_triggered:
        logger.warning("ALERT: Sudden temperature stability detected!")
    return alert_triggered

#####################################
# Function to process a single message
#####################################

def process_message(message: str, rolling_window: deque) -> None:
    try:
        logger.debug(f"Raw message: {message}")
        data = json.loads(message)
        temperature = data.get("temperature")
        timestamp = data.get("timestamp")
        if temperature is None or timestamp is None:
            logger.error(f"Invalid message format: {message}")
            return
        rolling_window.append(temperature)
        if detect_alert(rolling_window):
            logger.info(f"ALERT at {timestamp}: Temperature stable at {temperature}°F")
    except json.JSONDecodeError as e:
        logger.error(f"JSON decoding error: {e}")
    except Exception as e:
        logger.error(f"Error processing message: {e}")

#####################################
# Main Function
#####################################

def main() -> None:
    logger.info("Starting CSV Consumer")
    topic = get_kafka_topic()
    group_id = get_kafka_consumer_group_id()
    window_size = get_rolling_window_size()
    rolling_window = deque(maxlen=window_size)
    consumer = create_kafka_consumer(topic, group_id)
    logger.info(f"Polling messages from topic '{topic}'...")
    try:
        for message in consumer:
            message_str = message.value
            logger.debug(f"Received message: {message_str}")
            process_message(message_str, rolling_window)
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    finally:
        consumer.close()
        logger.info("Consumer closed.")

if __name__ == "__main__":
    main()
