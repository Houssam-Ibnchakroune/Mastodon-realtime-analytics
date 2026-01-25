"""
Simple script to stream Mastodon posts directly to terminal.
"""
from mastodon import StreamListener
from src.Mastodon_stream.producer.mastodon_client import MastodonClient


class TerminalHandler(StreamListener):
    """Simple handler that prints posts to terminal"""
    
    def on_update(self, status):
        """Called when a new post is received"""
        print("-" * 50)
        print(f"Author: @{status['account']['acct']}")
        print(f"Post: {status['content']}")
        print()


def main():
    """Stream Mastodon public timeline to terminal"""
    print("Starting Mastodon stream... Press Ctrl+C to stop")
    
    client = MastodonClient()
    client.stream_public_timeline(TerminalHandler())


if __name__ == "__main__":
    main()
