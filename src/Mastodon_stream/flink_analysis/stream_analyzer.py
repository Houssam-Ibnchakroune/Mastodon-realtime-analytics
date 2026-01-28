"""
PyFlink Stream Analyzer for Mastodon Data

This module provides real-time stream processing capabilities using PyFlink
to analyze Mastodon posts from Kafka.
"""
import os

# Set Java 17 for PyFlink (required for Flink 2.2.0)
# Must be set BEFORE importing pyflink modules
os.environ['JAVA_HOME'] = '/usr/lib/jvm/java-17-openjdk-amd64'
os.environ['PATH'] = f"{os.environ['JAVA_HOME']}/bin:{os.environ.get('PATH', '')}"

import json
import logging
from typing import Optional
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaOffsetsInitializer,
    KafkaSink,
    KafkaRecordSerializationSchema,
)
from pyflink.common import WatermarkStrategy, Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.functions import MapFunction, FilterFunction, ReduceFunction
from pyflink.datastream.window import TumblingProcessingTimeWindows
from pyflink.common.time import Time

logger = logging.getLogger(__name__)


class PostParser(MapFunction):
    """Parse JSON string to extract post data"""
    
    def map(self, value: str):
        try:
            data = json.loads(value)
            return json.dumps({
                'id': data.get('id'),
                'content': data.get('content', ''),
                'language': data.get('language', 'unknown'),
                'username': data.get('account', {}).get('username', ''),
                'followers_count': data.get('account', {}).get('followers_count', 0),
                'favourites_count': data.get('favourites_count', 0),
                'reblogs_count': data.get('reblogs_count', 0),
                'tags': data.get('tags', []),
                'created_at': data.get('created_at', ''),
            })
        except json.JSONDecodeError:
            return None


class LanguageFilter(FilterFunction):
    """Filter posts by language"""
    
    def __init__(self, languages: list = None):
        self.languages = languages or ['en', 'fr']
    
    def filter(self, value: str) -> bool:
        try:
            data = json.loads(value)
            lang = data.get('language', '')
            return lang in self.languages
        except:
            return False


class EngagementMapper(MapFunction):
    """Calculate engagement score for posts"""
    
    def map(self, value: str):
        try:
            data = json.loads(value)
            # Engagement score = favorites + (reblogs * 2) + replies
            engagement = (
                data.get('favourites_count', 0) +
                data.get('reblogs_count', 0) * 2 +
                data.get('replies_count', 0)
            )
            data['engagement_score'] = engagement
            return json.dumps(data)
        except:
            return value


class HashtagExtractor(MapFunction):
    """Extract hashtags from posts"""
    
    def map(self, value: str):
        try:
            data = json.loads(value)
            tags = data.get('tags', [])
            return json.dumps({
                'post_id': data.get('id'),
                'hashtags': tags,
                'hashtag_count': len(tags),
                'language': data.get('language', 'unknown'),
            })
        except:
            return None


class MastodonStreamAnalyzer:
    """
    PyFlink DataStream API analyzer for Mastodon posts.
    
    Provides real-time stream processing including:
    - Language filtering
    - Engagement scoring
    - Hashtag analysis
    - Window-based aggregations
    """
    
    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        input_topic: str = "mastodon-posts",
        output_topic: str = "mastodon-analyzed",
        group_id: str = "flink-mastodon-analyzer",
        parallelism: int = 1,
    ):
        """
        Initialize the Flink Stream Analyzer.
        
        Args:
            bootstrap_servers: Kafka broker addresses
            input_topic: Kafka topic to consume from
            output_topic: Kafka topic to produce results to
            group_id: Kafka consumer group ID
            parallelism: Flink job parallelism
        """
        self.bootstrap_servers = bootstrap_servers
        self.input_topic = input_topic
        self.output_topic = output_topic
        self.group_id = group_id
        self.parallelism = parallelism
        
        # Initialize Flink environment
        self.env = StreamExecutionEnvironment.get_execution_environment()
        self.env.set_parallelism(parallelism)
        
        # Add Kafka connector JAR
        jar_path = os.path.expanduser(
            "~/Downloads/flink-2.2.0/lib/flink-sql-connector-kafka-3.2.0-1.19.jar"
        )
        if os.path.exists(jar_path):
            self.env.add_jars(f"file://{jar_path}")
            logger.info(f"Added Kafka connector JAR: {jar_path}")
        else:
            logger.warning(f"Kafka connector JAR not found at {jar_path}")
    
    def _create_kafka_source(self) -> KafkaSource:
        """Create Kafka source connector"""
        return (
            KafkaSource.builder()
            .set_bootstrap_servers(self.bootstrap_servers)
            .set_topics(self.input_topic)
            .set_group_id(self.group_id)
            .set_starting_offsets(KafkaOffsetsInitializer.earliest())
            .set_value_only_deserializer(SimpleStringSchema())
            .build()
        )
    
    def _create_kafka_sink(self, topic: str = None) -> KafkaSink:
        """Create Kafka sink connector"""
        return (
            KafkaSink.builder()
            .set_bootstrap_servers(self.bootstrap_servers)
            .set_record_serializer(
                KafkaRecordSerializationSchema.builder()
                .set_topic(topic or self.output_topic)
                .set_value_serialization_schema(SimpleStringSchema())
                .build()
            )
            .build()
        )
    
    def analyze_engagement(self):
        """
        Analyze post engagement and output enriched data.
        
        Calculates engagement scores and filters for significant posts.
        """
        logger.info("Starting engagement analysis...")
        
        source = self._create_kafka_source()
        
        stream = (
            self.env
            .from_source(source, WatermarkStrategy.no_watermarks(), "Kafka Source")
            .map(PostParser())
            .filter(lambda x: x is not None)
            .map(EngagementMapper())
        )
        
        # Output to Kafka
        sink = self._create_kafka_sink("mastodon-engagement")
        stream.sink_to(sink)
        
        self.env.execute("Mastodon Engagement Analysis")
    
    def filter_by_language(self, languages: list = None):
        """
        Filter posts by language.
        
        Args:
            languages: List of language codes to include (default: ['en', 'fr'])
        """
        languages = languages or ['en', 'fr']
        logger.info(f"Starting language filter for: {languages}")
        
        source = self._create_kafka_source()
        
        stream = (
            self.env
            .from_source(source, WatermarkStrategy.no_watermarks(), "Kafka Source")
            .map(PostParser())
            .filter(lambda x: x is not None)
            .filter(LanguageFilter(languages))
        )
        
        sink = self._create_kafka_sink("mastodon-filtered")
        stream.sink_to(sink)
        
        self.env.execute("Mastodon Language Filter")
    
    def extract_hashtags(self):
        """
        Extract and analyze hashtags from posts.
        """
        logger.info("Starting hashtag extraction...")
        
        source = self._create_kafka_source()
        
        stream = (
            self.env
            .from_source(source, WatermarkStrategy.no_watermarks(), "Kafka Source")
            .map(HashtagExtractor())
            .filter(lambda x: x is not None)
        )
        
        sink = self._create_kafka_sink("mastodon-hashtags")
        stream.sink_to(sink)
        
        self.env.execute("Mastodon Hashtag Extraction")
    
    def run_full_pipeline(self):
        """
        Run complete analysis pipeline:
        1. Parse and validate posts
        2. Calculate engagement scores
        3. Extract hashtags
        4. Output enriched data
        """
        logger.info("Starting full analysis pipeline...")
        
        source = self._create_kafka_source()
        
        # Main processing stream
        parsed_stream = (
            self.env
            .from_source(source, WatermarkStrategy.no_watermarks(), "Kafka Source")
            .map(PostParser())
            .filter(lambda x: x is not None)
        )
        
        # Branch 1: Engagement analysis
        engagement_stream = parsed_stream.map(EngagementMapper())
        engagement_sink = self._create_kafka_sink("mastodon-engagement")
        engagement_stream.sink_to(engagement_sink)
        
        # Branch 2: Hashtag extraction
        hashtag_stream = parsed_stream.map(HashtagExtractor())
        hashtag_sink = self._create_kafka_sink("mastodon-hashtags")
        hashtag_stream.sink_to(hashtag_sink)
        
        self.env.execute("Mastodon Full Analysis Pipeline")


def main():
    """Run the stream analyzer"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Mastodon Flink Stream Analyzer')
    parser.add_argument('--bootstrap-servers', default='localhost:9092',
                        help='Kafka bootstrap servers')
    parser.add_argument('--input-topic', default='mastodon-posts',
                        help='Input Kafka topic')
    parser.add_argument('--analysis', choices=['engagement', 'language', 'hashtags', 'full'],
                        default='full', help='Type of analysis to run')
    parser.add_argument('--languages', nargs='+', default=['en', 'fr'],
                        help='Languages to filter (for language analysis)')
    
    args = parser.parse_args()
    
    analyzer = MastodonStreamAnalyzer(
        bootstrap_servers=args.bootstrap_servers,
        input_topic=args.input_topic,
    )
    
    if args.analysis == 'engagement':
        analyzer.analyze_engagement()
    elif args.analysis == 'language':
        analyzer.filter_by_language(args.languages)
    elif args.analysis == 'hashtags':
        analyzer.extract_hashtags()
    else:
        analyzer.run_full_pipeline()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
