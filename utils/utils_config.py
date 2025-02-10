"""
Config Utility
File: utils/utils_config.py

This script provides the configuration functions for the project. 

It centralizes the configuration management 
by loading environment variables from .env in the root project folder
 and constructing file paths using pathlib. 

If you rename any variables in .env, remember to:
- recopy .env to .env.example (and hide the secrets)
- update the corresponding function in this module.
"""

#####################################
# Imports
#####################################

# import from Python Standard Library
import os
import pathlib

# import from external packages
from dotenv import load_dotenv

# import from local modules
from .utils_logger import logger

#####################################
# Load Environment Variables
#####################################

load_dotenv()

#####################################
# Getter Functions for .env Variables
#####################################


def get_zookeeper_address() -> str:
    """Fetch ZOOKEEPER_ADDRESS from environment or use default."""
    address = os.getenv("ZOOKEEPER_ADDRESS", "127.0.0.1:2181")
    logger.info(f"ZOOKEEPER_ADDRESS: {address}")
    return address


def get_kafka_broker_address() -> str:
    """Fetch KAFKA_BROKER_ADDRESS from environment or use default."""
    address = os.getenv("KAFKA_BROKER_ADDRESS", "127.0.0.1:9092")
    logger.info(f"KAFKA_BROKER_ADDRESS: {address}")
    return address


def get_kafka_topic() -> str:
    """Fetch BUZZ_TOPIC from environment or use default."""
    topic = os.getenv("BUZZ_TOPIC", "buzzline")
    logger.info(f"BUZZ_TOPIC: {topic}")
    return topic


def get_message_interval_seconds_as_int() -> int:
    """Fetch MESSAGE_INTERVAL_SECONDS from environment or use default."""
    interval = int(os.getenv("MESSAGE_INTERVAL_SECONDS", 5))
    logger.info(f"MESSAGE_INTERVAL_SECONDS: {interval}")
    return interval


def get_kafka_consumer_group_id() -> str:
    """Fetch BUZZ_CONSUMER_GROUP_ID from environment or use default."""
    group_id = os.getenv("BUZZ_CONSUMER_GROUP_ID", "buzz_group")
    logger.info(f"BUZZ_CONSUMER_GROUP_ID: {group_id}")
    return group_id


def get_base_data_path() -> pathlib.Path:
    """Fetch BASE_DATA_DIR from environment or use default."""
    project_root = pathlib.Path(__file__).parent.parent
    data_dir = project_root / os.getenv("BASE_DATA_DIR", "data")
    logger.info(f"BASE_DATA_DIR: {data_dir}")
    return data_dir


def get_live_data_path() -> pathlib.Path:
    """Fetch LIVE_DATA_FILE_NAME from environment or use default."""
    live_data_path = get_base_data_path() / os.getenv(
        "LIVE_DATA_FILE_NAME", "project_live.json"
    )
    logger.info(f"LIVE_DATA_PATH: {live_data_path}")
    return live_data_path


def get_sqlite_path() -> pathlib.Path:
    """Fetch SQLITE_DB_FILE_NAME from environment or use default."""
    sqlite_path = get_base_data_path() / os.getenv("SQLITE_DB_FILE_NAME", "buzz.sqlite")
    logger.info(f"SQLITE_PATH: {sqlite_path}")
    return sqlite_path


def get_database_type() -> str:
    """Fetch DATABASE_TYPE from environment or use default."""
    db_type = os.getenv("DATABASE_TYPE", "sqlite")
    logger.info(f"DATABASE_TYPE: {db_type}")
    return db_type


def get_postgres_host() -> str:
    """Fetch POSTGRES_HOST from environment or use default."""
    host = os.getenv("POSTGRES_HOST", "localhost")
    logger.info(f"POSTGRES_HOST: {host}")
    return host


def get_postgres_port() -> int:
    """Fetch POSTGRES_PORT from environment or use default."""
    port = int(os.getenv("POSTGRES_PORT", 5432))
    logger.info(f"POSTGRES_PORT: {port}")
    return port


def get_postgres_db() -> str:
    """Fetch POSTGRES_DB from environment or use default."""
    db = os.getenv("POSTGRES_DB", "postgres_buzz_database")
    logger.info(f"POSTGRES_DB: {db}")
    return db


def get_postgres_user() -> str:
    """Fetch POSTGRES_USER from environment or use default."""
    user = os.getenv("POSTGRES_USER", "your_username")
    logger.info(f"POSTGRES_USER: {user}")
    return user


def get_postgres_password() -> str:
    """Fetch POSTGRES_PASSWORD from environment or use default."""
    password = os.getenv("POSTGRES_PASSWORD", "your_password")
    logger.info("POSTGRES_PASSWORD: [REDACTED]")
    return password


def get_mongodb_uri() -> str:
    """Fetch MONGODB_URI from environment or use default."""
    uri = os.getenv("MONGODB_URI", "mongodb://localhost:27017/")
    logger.info(f"MONGODB_URI: {uri}")
    return uri


def get_mongodb_db() -> str:
    """Fetch MONGODB_DB from environment or use default."""
    db = os.getenv("MONGODB_DB", "mongo_buzz_database")
    logger.info(f"MONGODB_DB: {db}")
    return db


def get_mongodb_collection() -> str:
    """Fetch MONGODB_COLLECTION from environment or use default."""
    collection = os.getenv("MONGODB_COLLECTION", "mongo_buzz_collection")
    logger.info(f"MONGODB_COLLECTION: {collection}")
    return collection


#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    # Test the configuration functions
    logger.info("Testing configuration.")
    try:
        get_zookeeper_address()
        get_kafka_broker_address()
        get_kafka_topic()
        get_message_interval_seconds_as_int()
        get_kafka_consumer_group_id()
        get_base_data_path()
        get_live_data_path()
        get_sqlite_path()
        get_database_type()
        get_postgres_host()
        get_postgres_port()
        get_postgres_db()
        get_postgres_user()
        get_postgres_password()
        get_mongodb_uri()
        get_mongodb_db()
        get_mongodb_collection()
        logger.info("SUCCESS: Configuration function tests complete.")

    except Exception as e:
        logger.error(f"ERROR: Configuration function test failed: {e}")
