import json
import sys
import time
import matplotlib.pyplot as plt
import numpy as np
from kafka import KafkaConsumer
from pymongo import MongoClient
from utils.utils_logger import logger
import utils.utils_config as config

# MongoDB configuration
MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "buzz_DB"
COLLECTION_NAME = "streaming data"

# Variable to store the dynamic average of sentiment
sentiment_scores = []

def init_db():
    """Initialize MongoDB by connecting and clearing old data."""
    logger.info(f"Initializing MongoDB for '{DB_NAME}' and collection '{COLLECTION_NAME}'.")
    try:
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]
        collection.drop()  # Optional: Clears previous data
        logger.info("SUCCESS: Database and collection initialized.")
    except Exception as e:
        logger.error(f"ERROR: Failed to initialize MongoDB: {e}")

def insert_message(message: dict) -> None:
    """Insert a filtered message into MongoDB."""
    if not message:
        return

    logger.info(f"Inserting filtered message: {message}")

    try:
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]
        collection.insert_one(message)
    except Exception as e:
        logger.error(f"ERROR: Failed to insert message into MongoDB: {e}")

def process_message(message: dict) -> dict:
    """Filter messages mentioning 'game' and store only the sentiment score."""
    logger.info(f"Processing message: {message}")
    
    try:
        keyword_mentioned = message.get("keyword_mentioned", "").lower()
        if "game" not in keyword_mentioned:
            return None
        
        sentiment = float(message.get("sentiment", 0.0))

        # Update the dynamic list of sentiment scores
        sentiment_scores.append(sentiment)
        
        # Calculate running average
        if sentiment_scores:
            avg_sentiment = np.mean(sentiment_scores)

        return {
            "sentiment": avg_sentiment  # Store only the updated average sentiment
        }
    except Exception as e:
        logger.error(f"Error processing message: {e}")
        return None

def consume_messages_from_kafka():
    """Consume messages from Kafka, process them, and store in MongoDB."""
    logger.info("Starting Kafka consumer...")

    try:
        topic = config.get_kafka_topic()
        kafka_url = config.get_kafka_broker_address()
        group_id = config.get_kafka_consumer_group_id()
        interval_secs = config.get_message_interval_seconds_as_int()

        consumer = KafkaConsumer(
            topic,
            group_id=group_id,
            bootstrap_servers=kafka_url,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
    except Exception as e:
        logger.error(f"ERROR: Could not create Kafka consumer: {e}")
        sys.exit(1)

    try:
        for message in consumer:
            processed_message = process_message(message.value)
            if processed_message:
                insert_message(processed_message)
            time.sleep(interval_secs)
    except Exception as e:
        logger.error(f"ERROR: Could not consume messages from Kafka: {e}")
        raise

def generate_sentiment_chart():
    """Generate a real-time updating sentiment trend line chart."""
    logger.info("Generating sentiment trend chart...")

    try:
        if not sentiment_scores:
            logger.warning("No sentiment data available to plot.")
            return

        # Log the sentiment scores to see what data you have
        logger.info(f"Sentiment scores: {sentiment_scores}")

        # Create the figure and axis objects only once
        if not hasattr(generate_sentiment_chart, 'ax'):
            generate_sentiment_chart.ax = plt.gca()  # Get current axes

        ax = generate_sentiment_chart.ax

        # Clear the previous plot (important for dynamic updates)
        ax.clear()

        # Plotting the sentiment trend
        ax.plot(sentiment_scores, marker="o", linestyle="-", color="b", label="Sentiment")
        ax.set_xlabel("Time (New Data Points)")
        ax.set_ylabel("Average Sentiment")
        ax.set_title("Real-Time Sentiment Trend for 'Game'-Related Messages")
        ax.legend()
        ax.grid(True)

        plt.draw()  # Redraw the plot with updated data
        plt.pause(0.1)  # Non-blocking update to the plot

    except Exception as e:
        logger.error(f"ERROR: Failed to generate sentiment chart: {e}")


def main():
    """Main function to initialize MongoDB and start Kafka consumer."""
    logger.info("Initializing MongoDB database.")
    init_db()

    logger.info("Start consuming Kafka messages and inserting into MongoDB.")
    try:
        # Enable interactive mode for dynamic plotting (add this line)
        plt.ion()  # Interactive mode on, for dynamic plotting

        while True:
            consume_messages_from_kafka()
            generate_sentiment_chart()  # Continuously update the sentiment chart
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        logger.info("Consumer shutting down.")
        plt.ioff()  # Turn off interactive mode when done
        plt.show()  # Show the final plot

if __name__ == "__main__":
    main()
