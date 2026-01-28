"""
PyFlink Table/SQL API Analyzer for Mastodon Data

This module provides SQL-based stream processing using PyFlink Table API
for more complex analytics on Mastodon posts.
"""
import os

# Set Java 17 for PyFlink (required for Flink 2.2.0)
# Must be set BEFORE importing pyflink modules
os.environ['JAVA_HOME'] = '/usr/lib/jvm/java-17-openjdk-amd64'
os.environ['PATH'] = f"{os.environ['JAVA_HOME']}/bin:{os.environ.get('PATH', '')}"

import logging
from pyflink.table import (
    EnvironmentSettings,
    TableEnvironment,
    DataTypes,
    Schema,
)
from pyflink.table.expressions import col, lit
from pyflink.table.window import Tumble

logger = logging.getLogger(__name__)


class MastodonSQLAnalyzer:
    """
    PyFlink Table/SQL API analyzer for Mastodon posts.
    
    Provides SQL-based analytics including:
    - Windowed aggregations
    - Language statistics
    - Trending hashtags
    - User activity analysis
    """
    
    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        input_topic: str = "mastodon-posts",
        group_id: str = "flink-sql-analyzer",
    ):
        """
        Initialize the SQL Analyzer.
        
        Args:
            bootstrap_servers: Kafka broker addresses
            input_topic: Kafka topic to consume from
            group_id: Kafka consumer group ID
        """
        self.bootstrap_servers = bootstrap_servers
        self.input_topic = input_topic
        self.group_id = group_id
        
        # Initialize Table Environment
        settings = EnvironmentSettings.in_streaming_mode()
        self.t_env = TableEnvironment.create(settings)
        
        # Configure checkpointing for fault tolerance
        self.t_env.get_config().set(
            "execution.checkpointing.interval", "60000"
        )
        
        # Add Kafka connector JAR
        jar_path = os.path.expanduser(
            "~/Downloads/flink-2.2.0/lib/flink-sql-connector-kafka-3.2.0-1.19.jar"
        )
        if os.path.exists(jar_path):
            self.t_env.get_config().set(
                "pipeline.jars", f"file://{jar_path}"
            )
            logger.info(f"Added Kafka connector JAR: {jar_path}")
    
    def _create_source_table(self, table_name: str = "mastodon_posts"):
        """Create Kafka source table with DDL"""
        
        create_source_ddl = f"""
            CREATE TABLE {table_name} (
                `id` STRING,
                `created_at` STRING,
                `content` STRING,
                `account` ROW<
                    `username` STRING,
                    `display_name` STRING,
                    `followers_count` BIGINT
                >,
                `language` STRING,
                `favourites_count` BIGINT,
                `reblogs_count` BIGINT,
                `replies_count` BIGINT,
                `url` STRING,
                `tags` ARRAY<STRING>,
                `event_time` AS TO_TIMESTAMP(created_at),
                WATERMARK FOR `event_time` AS `event_time` - INTERVAL '5' SECOND
            ) WITH (
                'connector' = 'kafka',
                'topic' = '{self.input_topic}',
                'properties.bootstrap.servers' = '{self.bootstrap_servers}',
                'properties.group.id' = '{self.group_id}',
                'scan.startup.mode' = 'earliest-offset',
                'format' = 'json',
                'json.fail-on-missing-field' = 'false',
                'json.ignore-parse-errors' = 'true'
            )
        """
        
        self.t_env.execute_sql(create_source_ddl)
        logger.info(f"Created source table: {table_name}")
    
    def _create_sink_table(self, table_name: str, topic: str, schema_ddl: str):
        """Create Kafka sink table with DDL"""
        
        create_sink_ddl = f"""
            CREATE TABLE {table_name} (
                {schema_ddl}
            ) WITH (
                'connector' = 'kafka',
                'topic' = '{topic}',
                'properties.bootstrap.servers' = '{self.bootstrap_servers}',
                'format' = 'json'
            )
        """
        
        self.t_env.execute_sql(create_sink_ddl)
        logger.info(f"Created sink table: {table_name}")
    
    def _create_print_sink(self, table_name: str, schema_ddl: str):
        """Create print sink for debugging"""
        
        create_sink_ddl = f"""
            CREATE TABLE {table_name} (
                {schema_ddl}
            ) WITH (
                'connector' = 'print'
            )
        """
        
        self.t_env.execute_sql(create_sink_ddl)
        logger.info(f"Created print sink: {table_name}")
    
    def analyze_language_distribution(self, window_minutes: int = 5):
        """
        Analyze language distribution over time windows.
        
        Args:
            window_minutes: Window size in minutes
        """
        logger.info(f"Analyzing language distribution (window: {window_minutes}m)...")
        
        self._create_source_table()
        
        # Create sink for results
        self._create_print_sink(
            "language_stats",
            """
            `window_start` TIMESTAMP(3),
            `window_end` TIMESTAMP(3),
            `language` STRING,
            `post_count` BIGINT,
            `total_engagement` BIGINT
            """
        )
        
        # SQL query for language distribution with tumbling window
        query = f"""
            INSERT INTO language_stats
            SELECT 
                TUMBLE_START(event_time, INTERVAL '{window_minutes}' MINUTE) AS window_start,
                TUMBLE_END(event_time, INTERVAL '{window_minutes}' MINUTE) AS window_end,
                COALESCE(language, 'unknown') AS language,
                COUNT(*) AS post_count,
                SUM(favourites_count + reblogs_count * 2) AS total_engagement
            FROM mastodon_posts
            GROUP BY 
                TUMBLE(event_time, INTERVAL '{window_minutes}' MINUTE),
                language
        """
        
        self.t_env.execute_sql(query)
    
    def analyze_top_users(self, window_minutes: int = 10):
        """
        Find most active users in time windows.
        
        Args:
            window_minutes: Window size in minutes
        """
        logger.info(f"Analyzing top users (window: {window_minutes}m)...")
        
        self._create_source_table()
        
        self._create_print_sink(
            "top_users",
            """
            `window_start` TIMESTAMP(3),
            `window_end` TIMESTAMP(3),
            `username` STRING,
            `post_count` BIGINT,
            `avg_followers` BIGINT
            """
        )
        
        query = f"""
            INSERT INTO top_users
            SELECT 
                TUMBLE_START(event_time, INTERVAL '{window_minutes}' MINUTE) AS window_start,
                TUMBLE_END(event_time, INTERVAL '{window_minutes}' MINUTE) AS window_end,
                account.username AS username,
                COUNT(*) AS post_count,
                AVG(account.followers_count) AS avg_followers
            FROM mastodon_posts
            WHERE account.username IS NOT NULL
            GROUP BY 
                TUMBLE(event_time, INTERVAL '{window_minutes}' MINUTE),
                account.username
            HAVING COUNT(*) > 1
        """
        
        self.t_env.execute_sql(query)
    
    def analyze_engagement_metrics(self, window_minutes: int = 5):
        """
        Calculate engagement metrics over time windows.
        
        Args:
            window_minutes: Window size in minutes
        """
        logger.info(f"Analyzing engagement metrics (window: {window_minutes}m)...")
        
        self._create_source_table()
        
        self._create_print_sink(
            "engagement_metrics",
            """
            `window_start` TIMESTAMP(3),
            `window_end` TIMESTAMP(3),
            `total_posts` BIGINT,
            `avg_favourites` DOUBLE,
            `avg_reblogs` DOUBLE,
            `max_engagement` BIGINT
            """
        )
        
        query = f"""
            INSERT INTO engagement_metrics
            SELECT 
                TUMBLE_START(event_time, INTERVAL '{window_minutes}' MINUTE) AS window_start,
                TUMBLE_END(event_time, INTERVAL '{window_minutes}' MINUTE) AS window_end,
                COUNT(*) AS total_posts,
                AVG(CAST(favourites_count AS DOUBLE)) AS avg_favourites,
                AVG(CAST(reblogs_count AS DOUBLE)) AS avg_reblogs,
                MAX(favourites_count + reblogs_count * 2) AS max_engagement
            FROM mastodon_posts
            GROUP BY 
                TUMBLE(event_time, INTERVAL '{window_minutes}' MINUTE)
        """
        
        self.t_env.execute_sql(query)
    
    def real_time_dashboard_query(self):
        """
        Create a comprehensive real-time analytics view.
        Outputs combined metrics for dashboard consumption.
        """
        logger.info("Starting real-time dashboard analytics...")
        
        self._create_source_table()
        
        # Create Kafka sink for dashboard data
        self._create_sink_table(
            "dashboard_output",
            "mastodon-dashboard",
            """
            `window_start` TIMESTAMP(3),
            `window_end` TIMESTAMP(3),
            `metric_type` STRING,
            `metric_name` STRING,
            `metric_value` DOUBLE
            """
        )
        
        # Combined metrics query
        query = """
            INSERT INTO dashboard_output
            SELECT 
                TUMBLE_START(event_time, INTERVAL '1' MINUTE) AS window_start,
                TUMBLE_END(event_time, INTERVAL '1' MINUTE) AS window_end,
                'volume' AS metric_type,
                'posts_per_minute' AS metric_name,
                CAST(COUNT(*) AS DOUBLE) AS metric_value
            FROM mastodon_posts
            GROUP BY 
                TUMBLE(event_time, INTERVAL '1' MINUTE)
        """
        
        self.t_env.execute_sql(query)
    
    def custom_sql_query(self, query: str):
        """
        Execute a custom SQL query on the Mastodon data.
        
        Args:
            query: SQL query string (SELECT only)
        """
        logger.info("Executing custom SQL query...")
        
        self._create_source_table()
        
        result = self.t_env.sql_query(query)
        result.execute().print()


def main():
    """Run the SQL analyzer"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Mastodon Flink SQL Analyzer')
    parser.add_argument('--bootstrap-servers', default='localhost:9092',
                        help='Kafka bootstrap servers')
    parser.add_argument('--input-topic', default='mastodon-posts',
                        help='Input Kafka topic')
    parser.add_argument('--analysis', 
                        choices=['language', 'users', 'engagement', 'dashboard'],
                        default='engagement', help='Type of analysis to run')
    parser.add_argument('--window', type=int, default=5,
                        help='Window size in minutes')
    
    args = parser.parse_args()
    
    analyzer = MastodonSQLAnalyzer(
        bootstrap_servers=args.bootstrap_servers,
        input_topic=args.input_topic,
    )
    
    if args.analysis == 'language':
        analyzer.analyze_language_distribution(args.window)
    elif args.analysis == 'users':
        analyzer.analyze_top_users(args.window)
    elif args.analysis == 'engagement':
        analyzer.analyze_engagement_metrics(args.window)
    elif args.analysis == 'dashboard':
        analyzer.real_time_dashboard_query()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
