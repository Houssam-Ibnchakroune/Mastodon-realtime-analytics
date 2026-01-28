"""
Main script to run Flink analysis on Mastodon stream data.

Usage:
    # DataStream API (simpler, more programmatic)
    uv run python run_flink_analysis.py --mode stream --analysis full
    
    # SQL API (complex queries, windowed aggregations)
    uv run python run_flink_analysis.py --mode sql --analysis engagement
"""
import os

# Set Java 17 for PyFlink (required for Flink 2.2.0)
# Must be set BEFORE importing pyflink modules
os.environ['JAVA_HOME'] = '/usr/lib/jvm/java-17-openjdk-amd64'
os.environ['PATH'] = f"{os.environ['JAVA_HOME']}/bin:{os.environ.get('PATH', '')}"

import argparse
import logging
import sys
from rich.console import Console
from rich.panel import Panel

console = Console()


def setup_logging(verbose: bool = False):
    """Configure logging"""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )


def run_stream_analysis(args):
    """Run DataStream API analysis"""
    from Mastodon_stream.flink_analysis import MastodonStreamAnalyzer
    
    analyzer = MastodonStreamAnalyzer(
        bootstrap_servers=args.bootstrap_servers,
        input_topic=args.input_topic,
        parallelism=args.parallelism,
    )
    
    console.print(f"[bold green]Starting DataStream Analysis: {args.analysis}[/bold green]")
    
    if args.analysis == 'engagement':
        analyzer.analyze_engagement()
    elif args.analysis == 'language':
        analyzer.filter_by_language(args.languages)
    elif args.analysis == 'hashtags':
        analyzer.extract_hashtags()
    elif args.analysis == 'full':
        analyzer.run_full_pipeline()


def run_sql_analysis(args):
    """Run Table/SQL API analysis"""
    from Mastodon_stream.flink_analysis import MastodonSQLAnalyzer
    
    analyzer = MastodonSQLAnalyzer(
        bootstrap_servers=args.bootstrap_servers,
        input_topic=args.input_topic,
    )
    
    console.print(f"[bold green]Starting SQL Analysis: {args.analysis}[/bold green]")
    
    if args.analysis == 'language':
        analyzer.analyze_language_distribution(args.window)
    elif args.analysis == 'users':
        analyzer.analyze_top_users(args.window)
    elif args.analysis == 'engagement':
        analyzer.analyze_engagement_metrics(args.window)
    elif args.analysis == 'dashboard':
        analyzer.real_time_dashboard_query()
    elif args.analysis == 'count':
        analyzer.simple_count_analysis()
    elif args.analysis == 'custom' and args.query:
        analyzer.custom_sql_query(args.query)


def main():
    parser = argparse.ArgumentParser(
        description='Flink Analysis for Mastodon Stream Data',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run full DataStream pipeline
  python run_flink_analysis.py --mode stream --analysis full
  
  # Run SQL engagement analysis with 5-minute windows
  python run_flink_analysis.py --mode sql --analysis engagement --window 5
  
  # Filter English and French posts
  python run_flink_analysis.py --mode stream --analysis language --languages en fr
        """
    )
    
    # Connection settings
    parser.add_argument('--bootstrap-servers', default='localhost:9092',
                        help='Kafka bootstrap servers (default: localhost:9092)')
    parser.add_argument('--input-topic', default='Mastodon_stream',
                        help='Input Kafka topic (default: Mastodon_stream)')
    
    # Analysis mode
    parser.add_argument('--mode', choices=['stream', 'sql'], default='stream',
                        help='Analysis mode: stream (DataStream API) or sql (Table API)')
    
    # Analysis type
    parser.add_argument('--analysis', 
                        choices=['engagement', 'language', 'hashtags', 'full', 'users', 'dashboard', 'count', 'custom'],
                        default='engagement',
                        help='Type of analysis to run')
    
    # Additional options
    parser.add_argument('--window', type=int, default=5,
                        help='Window size in minutes for SQL analysis (default: 5)')
    parser.add_argument('--languages', nargs='+', default=['en', 'fr'],
                        help='Languages to filter (default: en fr)')
    parser.add_argument('--parallelism', type=int, default=1,
                        help='Flink job parallelism (default: 1)')
    parser.add_argument('--query', type=str,
                        help='Custom SQL query (for --analysis custom)')
    parser.add_argument('-v', '--verbose', action='store_true',
                        help='Enable verbose logging')
    
    args = parser.parse_args()
    
    setup_logging(args.verbose)
    
    # Display configuration
    console.print(Panel.fit(
        f"""[bold]Flink Mastodon Analysis[/bold]
        
Mode: [cyan]{args.mode}[/cyan]
Analysis: [cyan]{args.analysis}[/cyan]
Kafka: [cyan]{args.bootstrap_servers}[/cyan]
Topic: [cyan]{args.input_topic}[/cyan]""",
        title="Configuration",
        border_style="blue"
    ))
    
    try:
        if args.mode == 'stream':
            run_stream_analysis(args)
        else:
            run_sql_analysis(args)
    except KeyboardInterrupt:
        console.print("\n[yellow]Analysis interrupted by user[/yellow]")
        sys.exit(0)
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        if args.verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
