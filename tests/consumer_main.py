"""
Example script for running the Kafka consumer with HDFS integration.
"""
import logging
from rich import print
from src.Mastodon_stream.consumer import MastodonKafkaConsumer, HDFSWriter

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


def main():
    """
    Main function to run the Kafka consumer and write to HDFS.
    """
    # Configuration
    KAFKA_TOPIC = 'Mastodon_stream'
    KAFKA_SERVERS = 'localhost:9092'
    HDFS_PATH = 'hdfs://localhost:9000/kafka_demo/tweets_data.json'
    
    # Initialize HDFS writer
    hdfs_writer = HDFSWriter(HDFS_PATH)
    
    # Initialize Kafka consumer
    consumer = MastodonKafkaConsumer(
        topic=KAFKA_TOPIC,
        bootstrap_servers=KAFKA_SERVERS,
        auto_offset_reset='latest',
        enable_auto_commit=True,
        group_id=None  # None for individual consumer
    )
    
    # Define callback to process messages
    def process_tweet(tweet):
        """Process each tweet: print and store to HDFS"""
        print(tweet)  # Print with rich formatting
        
        try:
            hdfs_writer.write_message(tweet)
            logger.info("Tweet stored in HDFS!")
        except Exception as e:
            logger.error(f"Failed to write tweet to HDFS: {e}")
    
    # Start consuming messages
    logger.info("Starting Kafka consumer...")
    consumer.consume_messages(callback=process_tweet)


if __name__ == "__main__":
    main()
