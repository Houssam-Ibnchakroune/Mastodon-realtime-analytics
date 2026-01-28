"""
Test script for Flink Mastodon Analyzer

This script provides a simple way to test the Flink analysis pipeline
without needing live Mastodon data.
"""
import os

# Set Java 17 for PyFlink (required for Flink 2.2.0)
# Must be set BEFORE importing pyflink modules
os.environ['JAVA_HOME'] = '/usr/lib/jvm/java-17-openjdk-amd64'
os.environ['PATH'] = f"{os.environ['JAVA_HOME']}/bin:{os.environ.get('PATH', '')}"

import json
import logging
import argparse
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

console = Console()
logging.basicConfig(level=logging.INFO)


def create_sample_posts():
    """Generate sample Mastodon posts for testing"""
    return [
        {
            "id": "1",
            "created_at": "2026-01-27T10:00:00Z",
            "content": "Hello world! Testing #Python #BigData",
            "account": {
                "username": "testuser1",
                "display_name": "Test User 1",
                "followers_count": 1000,
            },
            "language": "en",
            "favourites_count": 10,
            "reblogs_count": 5,
            "replies_count": 2,
            "url": "https://mastodon.social/@testuser1/1",
            "tags": ["Python", "BigData"],
        },
        {
            "id": "2",
            "created_at": "2026-01-27T10:01:00Z",
            "content": "Bonjour! Test en français #Flink",
            "account": {
                "username": "testuser2",
                "display_name": "Test User 2",
                "followers_count": 500,
            },
            "language": "fr",
            "favourites_count": 20,
            "reblogs_count": 10,
            "replies_count": 5,
            "url": "https://mastodon.social/@testuser2/2",
            "tags": ["Flink"],
        },
        {
            "id": "3",
            "created_at": "2026-01-27T10:02:00Z",
            "content": "Apache Flink is great for stream processing! #ApacheFlink #Streaming",
            "account": {
                "username": "dataengineer",
                "display_name": "Data Engineer",
                "followers_count": 5000,
            },
            "language": "en",
            "favourites_count": 100,
            "reblogs_count": 50,
            "replies_count": 20,
            "url": "https://mastodon.social/@dataengineer/3",
            "tags": ["ApacheFlink", "Streaming"],
        },
    ]


def test_engagement_calculation(posts):
    """Test engagement score calculation"""
    console.print("\n[bold blue]Testing Engagement Calculation[/bold blue]")
    
    table = Table(title="Engagement Scores")
    table.add_column("Username", style="cyan")
    table.add_column("Favourites", justify="right")
    table.add_column("Reblogs", justify="right")
    table.add_column("Replies", justify="right")
    table.add_column("Engagement Score", justify="right", style="green")
    
    for post in posts:
        engagement = (
            post['favourites_count'] +
            post['reblogs_count'] * 2 +
            post['replies_count']
        )
        table.add_row(
            post['account']['username'],
            str(post['favourites_count']),
            str(post['reblogs_count']),
            str(post['replies_count']),
            str(engagement),
        )
    
    console.print(table)


def test_language_filter(posts, languages=['en', 'fr']):
    """Test language filtering"""
    console.print(f"\n[bold blue]Testing Language Filter: {languages}[/bold blue]")
    
    filtered = [p for p in posts if p.get('language') in languages]
    
    table = Table(title="Filtered Posts")
    table.add_column("ID", style="cyan")
    table.add_column("Language", style="yellow")
    table.add_column("Username")
    table.add_column("Content Preview")
    
    for post in filtered:
        table.add_row(
            post['id'],
            post['language'],
            post['account']['username'],
            post['content'][:50] + "..." if len(post['content']) > 50 else post['content'],
        )
    
    console.print(table)
    console.print(f"[green]Filtered {len(filtered)} posts out of {len(posts)}[/green]")


def test_hashtag_extraction(posts):
    """Test hashtag extraction"""
    console.print("\n[bold blue]Testing Hashtag Extraction[/bold blue]")
    
    all_tags = {}
    for post in posts:
        for tag in post.get('tags', []):
            all_tags[tag] = all_tags.get(tag, 0) + 1
    
    table = Table(title="Hashtag Frequency")
    table.add_column("Hashtag", style="cyan")
    table.add_column("Count", justify="right", style="green")
    
    for tag, count in sorted(all_tags.items(), key=lambda x: -x[1]):
        table.add_row(f"#{tag}", str(count))
    
    console.print(table)


def test_flink_import():
    """Test PyFlink import"""
    console.print("\n[bold blue]Testing PyFlink Import[/bold blue]")
    
    try:
        from pyflink.datastream import StreamExecutionEnvironment
        from pyflink.table import TableEnvironment, EnvironmentSettings
        console.print("[green]✅ PyFlink imported successfully![/green]")
        
        # Test environment creation
        env = StreamExecutionEnvironment.get_execution_environment()
        console.print("[green]✅ StreamExecutionEnvironment created![/green]")
        
        settings = EnvironmentSettings.in_streaming_mode()
        t_env = TableEnvironment.create(settings)
        console.print("[green]✅ TableEnvironment created![/green]")
        
        return True
    except ImportError as e:
        console.print(f"[red]❌ PyFlink import failed: {e}[/red]")
        console.print("[yellow]Make sure apache-flink is installed: uv add apache-flink[/yellow]")
        return False
    except Exception as e:
        console.print(f"[red]❌ Error: {e}[/red]")
        return False


def test_analyzer_import():
    """Test analyzer module import"""
    console.print("\n[bold blue]Testing Analyzer Import[/bold blue]")
    
    try:
        from Mastodon_stream.flink_analysis import MastodonStreamAnalyzer, MastodonSQLAnalyzer
        console.print("[green]✅ Analyzers imported successfully![/green]")
        return True
    except ImportError as e:
        console.print(f"[red]❌ Import failed: {e}[/red]")
        return False


def main():
    parser = argparse.ArgumentParser(description='Test Flink Analyzer')
    parser.add_argument('--full', action='store_true', help='Run full tests including Flink')
    args = parser.parse_args()
    
    console.print(Panel.fit(
        "[bold green]Mastodon Flink Analyzer Tests[/bold green]",
        border_style="green"
    ))
    
    # Generate sample data
    posts = create_sample_posts()
    console.print(f"\n[dim]Generated {len(posts)} sample posts for testing[/dim]")
    
    # Run tests
    test_engagement_calculation(posts)
    test_language_filter(posts)
    test_hashtag_extraction(posts)
    
    if args.full:
        test_flink_import()
        test_analyzer_import()
    
    console.print("\n[bold green]All tests completed![/bold green]")


if __name__ == "__main__":
    main()
