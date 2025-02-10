"""
utils_consumer.py - common functions used by consumers.

Consumers subscribe to a topic and read messages from the Kafka topic.
"""

#####################################
# Imports
#####################################


# Import external packages
from kafka import KafkaConsumer

# Import functions from local modules
from .utils_config import get_kafka_broker_address
from .utils_logger import logger


#####################################
# Helper Functions
#####################################


def create_kafka_consumer(
    topic_provided: str = None,
    group_id_provided: str = None,
    value_deserializer_provided=None,
):
    """
    Create and return a Kafka consumer instance.

    Args:
        topic_provided (str): The Kafka topic to subscribe to. Defaults to the environment variable or default.
        group_id_provided (str): The consumer group ID. Defaults to the environment variable or default.
        value_deserializer_provided (callable, optional): Function to deserialize message values.

    Returns:
        KafkaConsumer: Configured Kafka consumer instance.
    """
    kafka_broker = get_kafka_broker_address()
    topic = topic_provided
    consumer_group_id = group_id_provided or "test_group"
    logger.info(
        f"Creating Kafka consumer. Topic='{topic}' and group ID='{group_id_provided}'."
    )
    logger.debug(f"Kafka broker: {kafka_broker}")

    try:
        consumer = KafkaConsumer(
            topic,
            group_id=consumer_group_id,
            value_deserializer=value_deserializer_provided
            or (lambda x: x.decode("utf-8")),
            bootstrap_servers=kafka_broker,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
        )
        logger.info("Kafka consumer created successfully.")
        return consumer
    except Exception as e:
        logger.error(f"Error creating Kafka consumer: {e}")
        raise
