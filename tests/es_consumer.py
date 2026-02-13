"""
Elasticsearch consumer for Flink analysis results.

Reads from Kafka topics (mastodon-engagement, mastodon-hashtags)
produced by Flink analysis and stores them in Elasticsearch.

Usage:
    uv run python -m tests.es_consumer
    uv run python -m tests.es_consumer --topics engagement hashtags
    uv run python -m tests.es_consumer --topics engagement
"""
import json
import logging
import signal
import sys
import threading
from confluent_kafka import Consumer, KafkaError
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.live import Live
from src.Mastodon_stream.consumer import ElasticsearchWriter

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)
console = Console()


class FlinkToElasticsearchConsumer:
    """
    Consumes Flink analysis results from Kafka and stores them in Elasticsearch.
    
    Listens to:
    - mastodon-engagement: Posts with engagement scores
    - mastodon-hashtags: Extracted hashtags
    """
    
    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        es_hosts: str = "http://localhost:9200",
        topics: list = None,
    ):
        self.bootstrap_servers = bootstrap_servers
        self.topics = topics or ["mastodon-engagement", "mastodon-hashtags"]
        self.running = True
        
        # Counters
        self.engagement_count = 0
        self.hashtags_count = 0
        self.error_count = 0
        
        # Initialize Elasticsearch writer
        self.es_writer = ElasticsearchWriter(hosts=es_hosts)
        
        # Initialize Kafka consumer
        config = {
            'bootstrap.servers': bootstrap_servers,
            'group.id': 'flink-es-consumer',
            'auto.offset.reset': 'latest',
            'enable.auto.commit': True,
        }
        self.consumer = Consumer(config)
        self.consumer.subscribe(self.topics)
        
        # Handle graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals."""
        console.print("\n[yellow]⚠️  Arrêt en cours...[/yellow]")
        self.running = False
    
    def _process_message(self, topic: str, data: dict):
        """Process a message based on its topic."""
        if topic == "mastodon-engagement":
            if self.es_writer.index_engagement(data):
                self.engagement_count += 1
                username = data.get("username", "?")
                score = data.get("engagement_score", 0)
                lang = data.get("language", "?")
                console.print(
                    f"  📊 [green]Engagement[/green] | "
                    f"@{username} | score: {score} | "
                    f"lang: {lang} | "
                    f"[dim]total: {self.engagement_count}[/dim]"
                )
            else:
                self.error_count += 1
                
        elif topic == "mastodon-hashtags":
            if self.es_writer.index_hashtags(data):
                self.hashtags_count += 1
                tags = data.get("hashtags", [])
                count = data.get("hashtag_count", 0)
                console.print(
                    f"  🏷️  [blue]Hashtags[/blue]   | "
                    f"tags: {tags[:5]} | count: {count} | "
                    f"[dim]total: {self.hashtags_count}[/dim]"
                )
            else:
                self.error_count += 1
    
    def run(self):
        """Start consuming and indexing."""
        console.print(Panel.fit(
            f"""[bold]Flink → Elasticsearch Consumer[/bold]
            
Kafka: [cyan]{self.bootstrap_servers}[/cyan]
Topics: [cyan]{', '.join(self.topics)}[/cyan]
Elasticsearch: [cyan]{self.es_writer.es.info()['version']['number']}[/cyan]
Indices: [cyan]{self.es_writer.engagement_index}, {self.es_writer.hashtags_index}[/cyan]""",
            title="Configuration",
            border_style="green"
        ))
        
        console.print("\n[bold green]🚀 En attente de données Flink...[/bold green]")
        console.print("[dim]Appuyez sur Ctrl+C pour arrêter[/dim]\n")
        
        try:
            while self.running:
                msg = self.consumer.poll(timeout=1.0)
                
                if msg is None:
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        logger.error(f"Consumer error: {msg.error()}")
                        self.error_count += 1
                        continue
                
                try:
                    value = msg.value().decode('utf-8')
                    data = json.loads(value)
                    topic = msg.topic()
                    self._process_message(topic, data)
                    
                except json.JSONDecodeError as e:
                    logger.warning(f"Invalid JSON: {e}")
                    self.error_count += 1
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    self.error_count += 1
                    
        finally:
            self._shutdown()
    
    def _shutdown(self):
        """Clean shutdown."""
        self.consumer.close()
        
        # Print final stats
        stats = self.es_writer.get_stats()
        self.es_writer.close()
        
        console.print("\n")
        table = Table(title="📈 Résumé Final")
        table.add_column("Métrique", style="cyan")
        table.add_column("Valeur", style="green", justify="right")
        
        table.add_row("Posts engagement indexés (session)", str(self.engagement_count))
        table.add_row("Hashtags indexés (session)", str(self.hashtags_count))
        table.add_row("Erreurs", str(self.error_count))
        table.add_row("─" * 30, "─" * 10)
        table.add_row("Total docs mastodon-engagement", str(stats.get("mastodon-engagement", 0)))
        table.add_row("Total docs mastodon-hashtags", str(stats.get("mastodon-hashtags", 0)))
        
        console.print(table)
        console.print("[green]✅ Consumer arrêté proprement[/green]")


def main():
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Consume Flink results and store in Elasticsearch'
    )
    parser.add_argument('--bootstrap-servers', default='localhost:9092',
                        help='Kafka bootstrap servers')
    parser.add_argument('--es-hosts', default='http://localhost:9200',
                        help='Elasticsearch hosts')
    parser.add_argument('--topics', nargs='+',
                        choices=['engagement', 'hashtags'],
                        default=['engagement', 'hashtags'],
                        help='Topics to consume (default: both)')
    
    args = parser.parse_args()
    
    # Map short names to full topic names
    topic_map = {
        'engagement': 'mastodon-engagement',
        'hashtags': 'mastodon-hashtags',
    }
    topics = [topic_map[t] for t in args.topics]
    
    consumer = FlinkToElasticsearchConsumer(
        bootstrap_servers=args.bootstrap_servers,
        es_hosts=args.es_hosts,
        topics=topics,
    )
    consumer.run()


if __name__ == "__main__":
    main()
