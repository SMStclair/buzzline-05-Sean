"""
utils_producer.py - common functions used by producers.

Producers send messages to a Kafka topic.
"""

#####################################
# Import Modules
#####################################

# Import packages from Python Standard Library
import sys
import socket
import time

# Import external packages
from kafka import KafkaProducer, KafkaConsumer, errors
from kafka.admin import (
    KafkaAdminClient,
    ConfigResource,
    ConfigResourceType,
    NewTopic,
)

# Import functions from local modules
from .utils_config import get_zookeeper_address, get_kafka_broker_address
from .utils_logger import logger

#####################################
# Kafka and Zookeeper Readiness Checks
#####################################


def check_zookeeper_service_is_ready():
    """
    Check if Zookeeper is ready by verifying its port is open.

    Returns:
        bool: True if Zookeeper is ready, False otherwise.
    """
    zookeeper_address = get_zookeeper_address()
    host, port = zookeeper_address.split(":")
    port = int(port)

    try:
        with socket.create_connection((host, port), timeout=5):
            logger.info(f"Zookeeper is ready at {host}:{port}.")
            return True
    except Exception as e:
        logger.error(f"Error checking Zookeeper readiness: {e}")
        return False


def check_kafka_service_is_ready():
    """
    Check if Kafka is ready by connecting to the broker and fetching metadata.

    Returns:
        bool: True if Kafka is ready, False otherwise.
    """
    kafka_broker = get_kafka_broker_address()

    try:
        admin_client = KafkaAdminClient(bootstrap_servers=kafka_broker)
        brokers = admin_client.describe_cluster()
        logger.info(f"Kafka is ready. Brokers: {brokers}")
        admin_client.close()
        return True
    except errors.KafkaError as e:
        logger.error(f"Error checking Kafka: {e}")
        return False


#####################################
# Verify Zookeeper and Kafka Services
#####################################


def verify_services():
    # Verify Zookeeper is ready
    if not check_zookeeper_service_is_ready():
        logger.error(
            "Zookeeper is not ready. Please check your Zookeeper setup. Exiting..."
        )
        sys.exit(1)

    # Verify Kafka is ready
    if not check_kafka_service_is_ready():
        logger.error(
            "Kafka broker is not ready. Please check your Kafka setup. Exiting..."
        )
        sys.exit(2)


#####################################
# Create a Kafka Producer
#####################################


def create_kafka_producer(value_serializer=None):
    """
    Create and return a Kafka producer instance.

    Args:
        value_serializer (callable): A custom serializer for message values.
                                     Defaults to UTF-8 string encoding.

    Returns:
        KafkaProducer: Configured Kafka producer instance.
    """
    kafka_broker = get_kafka_broker_address()

    if value_serializer is None:

        def value_serializer(x):
            return x.encode("utf-8")  # Default to string serialization

    try:
        logger.info(f"Connecting to Kafka broker at {kafka_broker}...")
        producer = KafkaProducer(
            bootstrap_servers=kafka_broker,
            value_serializer=value_serializer,
        )
        logger.info("Kafka producer successfully created.")
        return producer
    except Exception as e:
        logger.error(f"Failed to create Kafka producer: {e}")
        return None


#####################################
# Create a Kafka Topic
#####################################


def create_kafka_topic(topic_name, group_id=None):
    """
    Create a fresh Kafka topic with the given name.
    Args:
        topic_name (str): Name of the Kafka topic.
    """
    kafka_broker = get_kafka_broker_address()

    try:
        admin_client = KafkaAdminClient(bootstrap_servers=kafka_broker)

        # Check if the topic exists
        topics = admin_client.list_topics()
        if topic_name in topics:
            logger.info(f"Topic '{topic_name}' exists. Clearing it out...")
            clear_kafka_topic(topic_name, group_id)

        else:
            logger.info(f"Creating '{topic_name}'.")
            new_topic = NewTopic(
                name=topic_name, num_partitions=1, replication_factor=1
            )
            admin_client.create_topics([new_topic])
            logger.info(f"Topic '{topic_name}' created successfully.")

    except Exception as e:
        logger.error(f"Error managing topic '{topic_name}': {e}")
        sys.exit(1)

    finally:
        admin_client.close()


#####################################
# Clear a Kafka Topic
#####################################


def clear_kafka_topic(topic_name, group_id):
    """
    Consume and discard all messages in the Kafka topic to clear it.

    Args:
        topic_name (str): Name of the Kafka topic.
        group_id (str): Consumer group ID.
    """
    kafka_broker = get_kafka_broker_address()
    admin_client = KafkaAdminClient(bootstrap_servers=kafka_broker)

    try:
        # Fetch the current retention period
        config_resource = ConfigResource(ConfigResourceType.TOPIC, topic_name)
        configs = admin_client.describe_configs([config_resource])
        original_retention = configs[config_resource].get(
            "retention.ms", "604800000"
        )  # Default to 7 days
        logger.info(
            f"Original retention.ms for topic '{topic_name}': {original_retention}"
        )

        # Temporarily set retention to 1ms
        admin_client.alter_configs({config_resource: {"retention.ms": "1"}})
        logger.info(f"Retention.ms temporarily set to 1ms for topic '{topic_name}'.")

        # Wait a moment for Kafka to apply retention and delete old data
        time.sleep(2)

        # Clear remaining messages by consuming and discarding them
        logger.info(f"Clearing topic '{topic_name}' by consuming all messages...")
        consumer = KafkaConsumer(
            topic_name,
            group_id=group_id,
            bootstrap_servers=kafka_broker,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
        )
        for message in consumer:
            logger.debug(f"Clearing message: {message.value}")
        consumer.close()
        logger.info(f"All messages cleared from topic '{topic_name}'.")

        # Restore the original retention period
        admin_client.alter_configs(
            {config_resource: {"retention.ms": original_retention}}
        )
        logger.info(
            f"Retention.ms restored to {original_retention} for topic '{topic_name}'."
        )

    except Exception as e:
        logger.error(f"Error managing retention for topic '{topic_name}': {e}")
    finally:
        admin_client.close()


#####################################
# Find Out if a Kafka Topic Exists
#####################################


def is_topic_available(topic_name) -> bool:
    """
    Verify a kafka topic exists with the given name.
    Args:
        topic_name (str): Name of the Kafka topic.
    Returns:
        bool: True if the topic exists, False otherwise.
    """
    kafka_broker = get_kafka_broker_address()

    try:
        admin_client = KafkaAdminClient(bootstrap_servers=kafka_broker)

        # Check if the topic exists
        topics = admin_client.list_topics()
        if topic_name in topics:
            logger.info(f"Topic '{topic_name}' already exists. ")
            return True
        else:
            logger.error(f"Topic '{topic_name}' does not exist.")
            return False

    except Exception as e:
        logger.error(f"Error verifying topic '{topic_name}': {e}")
        sys.exit(8)

    finally:
        admin_client.close()


#####################################
# Main Function for Testing
#####################################


def main():
    """
    Main entry point.
    """
    if not check_zookeeper_service_is_ready():
        logger.error(
            "Zookeeper is not ready. Check .env file and ensure Zookeeper is running."
        )
        sys.exit(1)

    if not check_kafka_service_is_ready():
        logger.error("Kafka is not ready. Check .env file and ensure Kafka is running.")
        sys.exit(2)

    logger.info("All services are ready. Proceed with producer setup.")
    create_kafka_topic("test_topic", "default_group")


#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()
