# test_pipeline.py
import logging
from Mastodon_stream.producer.mastodon_client import MastodonClient
from Mastodon_stream.producer.kafka_producer import KafkaProducer
from Mastodon_stream.producer.stream_handler import StreamHandler

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def main():
    print("🚀 Démarrage du pipeline Mastodon → Kafka\n")
    
    # 1. Initialiser le producer Kafka
    print("📨 Connexion à Kafka...")
    kafka_producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        topic='Mastodon_stream'
    )
    print("✅ Kafka connecté\n")
    
    # 2. Créer le handler avec le producer
    handler = StreamHandler(kafka_producer)
    
    # 3. Initialiser le client Mastodon
    print("🐘 Connexion à Mastodon...")
    mastodon_client = MastodonClient(instance_url="https://mastodon.social")
    print("✅ Mastodon connecté\n")
    
    # 4. Démarrer le streaming
    print("🔄 Démarrage du stream en temps réel...")
    print("📊 Les messages apparaîtront dans le consumer Kafka")
    print("⏹️  Appuyez sur Ctrl+C pour arrêter\n")
    print("-" * 60)
    
    try:
        mastodon_client.stream_public_timeline(handler)
    except KeyboardInterrupt:
        print("\n\n⏹️  Arrêt du stream...")
        print(f"📊 Total de messages traités: {handler.messages_count}")
    except Exception as e:
        print(f"\n❌ Erreur: {e}")

if __name__ == "__main__":
    main()