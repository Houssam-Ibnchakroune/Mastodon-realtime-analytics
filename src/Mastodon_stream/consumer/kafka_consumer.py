from confluent_kafka import Consumer, KafkaError
import json
import logging

logger = logging.getLogger(__name__)


class MastodonKafkaConsumer:
    """
    Kafka consumer for consuming Mastodon tweets from Kafka topic.
    """
    
    def __init__(
        self,
        topic: str,
        bootstrap_servers: str,
        group_id: str = None,
        auto_offset_reset: str = 'earliest',
        enable_auto_commit: bool = True
    ):
        """
        Initialize Kafka consumer.
        
        Args:
            topic: Kafka topic to consume from
            bootstrap_servers: Comma-separated list of Kafka broker addresses
            group_id: Consumer group ID (None for individual consumer)
            auto_offset_reset: Where to start reading ('earliest' or 'latest')
            enable_auto_commit: Whether to auto-commit offsets
        """
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers
        
        config = {
            'bootstrap.servers': bootstrap_servers,
            'auto.offset.reset': auto_offset_reset,
            'enable.auto.commit': enable_auto_commit,
        }
        
        if group_id:
            config['group.id'] = group_id
        else:
            # Generate unique group ID for individual consumer
            import uuid
            config['group.id'] = f'consumer-{uuid.uuid4()}'
        
        try:
            self.consumer = Consumer(config)
            self.consumer.subscribe([topic])
            logger.info(f"Kafka consumer initialized for topic: {topic}")
        except Exception as e:
            logger.error(f"Failed to initialize Kafka consumer: {e}")
            raise

    def consume_messages(self, callback=None, timeout=1.0):
        """
        Start consuming messages from Kafka topic.
        
        Args:
            callback: Optional callback function to process each message.
                     Should accept message value as parameter.
            timeout: Consumer poll timeout in seconds
        """
        logger.info(f"Starting to consume messages from topic: {self.topic}")
        
        try:
            while True:
                msg = self.consumer.poll(timeout=timeout)
                
                if msg is None:
                    continue
                    
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.info(f"Reached end of partition {msg.partition()}")
                    else:
                        logger.error(f"Consumer error: {msg.error()}")
                    continue
                
                try:
                    # Deserialize the message value
                    tweet = json.loads(msg.value().decode('utf-8'))
                    
                    if callback:
                        callback(tweet)
                    else:
                        logger.info(f"Received message: {tweet}")
                        
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to decode message: {e}")
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    
        except KeyboardInterrupt:
            logger.info("Consumer interrupted by user")
        except Exception as e:
            logger.error(f"Error consuming messages: {e}")
            raise
        finally:
            self.close()

    def close(self):
        """Close the Kafka consumer connection."""
        if self.consumer:
            self.consumer.close()
            logger.info("Kafka consumer closed")
