import json
import logging
from mastodon import StreamListener
from typing import Dict, Any

logger = logging.getLogger(__name__)


class StreamHandler(StreamListener):
    """
    Handler pour traiter les événements du stream Mastodon
    et les envoyer vers Kafka
    """
    
    def __init__(self, kafka_producer):
        """
        Initialise le handler avec un producer Kafka
        
        Args:
            kafka_producer: Instance de KafkaProducer pour envoyer les messages
        """
        super().__init__()
        self.kafka_producer = kafka_producer
        self.messages_count = 0
    
    def on_update(self, status):
        """
        Appelé quand un nouveau post/toot arrive
        
        Args:
            status: Objet status de Mastodon contenant le post
        """
        try:
            # Extraire les données importantes
            post_data = self._extract_post_data(status)
            
            # Convertir en JSON
            message = json.dumps(post_data, ensure_ascii=False)
            
            # Envoyer vers Kafka
            key = str(status['id'])
            self.kafka_producer.send_message(key=key, value=message)
            
            self.messages_count += 1
            logger.info(f"✅ Post envoyé à Kafka | Total: {self.messages_count}")
            
        except Exception as e:
            logger.error(f"❌ Erreur lors du traitement du post: {e}")
    
    def on_notification(self, notification):
        """Appelé lors d'une notification (optionnel)"""
        logger.debug(f"Notification reçue: {notification['type']}")
    
    def on_delete(self, status_id):
        """Appelé quand un post est supprimé (optionnel)"""
        logger.info(f"Post supprimé: {status_id}")
    
    def on_abort(self, err):
        """
        Appelé en cas d'erreur de connexion.
        Retourne False pour ignorer l'erreur et continuer le stream.
        """
        logger.warning(f"⚠️ Erreur stream (ignorée): {err}")
        # Return False to NOT raise the exception and keep streaming
        return False
    
    def on_unknown_event(self, name, unknown_event=None):
        """
        Appelé quand un événement inconnu est reçu.
        Ignore silencieusement les événements malformés.
        """
        logger.debug(f"Événement inconnu ignoré: {name}")
    
    def handle_heartbeat(self):
        """Appelé lors des heartbeats - garde la connexion vivante"""
        pass
    
    def _extract_post_data(self, status: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extrait les données pertinentes d'un post Mastodon
        
        Args:
            status: Objet status complet de Mastodon
            
        Returns:
            Dictionnaire avec les données filtrées
        """
        return {
            "id": status['id'],
            "created_at": status['created_at'].isoformat() if hasattr(status['created_at'], 'isoformat') else str(status['created_at']),
            "content": status['content'],
            "account": {
                "username": status['account']['username'],
                "display_name": status['account']['display_name'],
                "followers_count": status['account']['followers_count'],
            },
            "language": status.get('language'),
            "favourites_count": status.get('favourites_count', 0),
            "reblogs_count": status.get('reblogs_count', 0),
            "replies_count": status.get('replies_count', 0),
            "url": status.get('url'),
            "tags": [tag['name'] for tag in status.get('tags', [])],
        }
