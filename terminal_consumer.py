"""
Simple standalone script to stream Mastodon posts to terminal.
"""
import os
from dotenv import load_dotenv
from mastodon import Mastodon, CallbackStreamListener

# Load environment variables
load_dotenv()

# Get credentials from .env
client_key = os.getenv("CLIENT_KEY")
client_secret = os.getenv("CLIENT_SECRET")
access_token = os.getenv("ACCESS_TOKEN")

# Create Mastodon client
client = Mastodon(
    client_id=client_key,
    client_secret=client_secret,
    access_token=access_token,
    api_base_url="https://mastodon.social"
)

# Simple function to handle posts
def on_update(status):
    print("-" * 50)
    print(f"@{status['account']['acct']}: {status['content']}")

# Start streaming
print("Streaming Mastodon public timeline... Press Ctrl+C to stop")
client.stream_public(CallbackStreamListener(update_handler=on_update))
