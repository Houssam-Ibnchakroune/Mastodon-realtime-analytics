# src/Mastodon_stream/__init__.py
"""Pipeline Mastodon vers Kafka"""

__version__ = "0.1.0"

# Exports publics
from .producer.mastodon_client import MastodonClient
#from .producer.kafka_producer import KafkaProducer
#from .producer.stream_handler import StreamHandler

__all__ = ["MastodonClient", "KafkaProducer", "StreamHandler"]