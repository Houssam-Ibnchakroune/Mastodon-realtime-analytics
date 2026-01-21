from confluent_kafka import Producer
import logging
logger = logging.getLogger(__name__)
class KafkaProducer:
    def __init__(self, bootstrap_servers: str, topic: str):
        self.topic = topic
        self.producer = Producer({'bootstrap.servers': bootstrap_servers})

    def delivery_report(self, err, msg):
        if err is not None:
            logger.error(f"Message delivery failed: {err}")
        else:
            logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}]")

    def send_message(self, key: str, value: str):
        try:
            self.producer.produce(self.topic, key=key, value=value, callback=self.delivery_report)
            self.producer.flush()
        except Exception as e:
            logger.error(f"Failed to send message: {e}")