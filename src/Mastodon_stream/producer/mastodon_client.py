import os
import time
import logging
from dotenv import load_dotenv
from mastodon import Mastodon

# Charger les variables d'environnement
load_dotenv()

logger = logging.getLogger(__name__)


class MastodonClient:
    """Client pour se connecter à l'API Mastodon"""
    
    def __init__(self, instance_url: str = "https://mastodon.social"):
        """
        Initialise le client Mastodon
        
        Args:
            instance_url: URL de l'instance Mastodon (par défaut mastodon.social)
        """
        self.instance_url = instance_url
        self.client = None
        self._setup_client()
    
    def _setup_client(self):
        """Configure le client Mastodon avec les credentials du .env"""
        client_key = os.getenv("CLIENT_KEY")
        client_secret = os.getenv("CLIENT_SECRET")
        access_token = os.getenv("ACCESS_TOKEN")
        
        # Vérifier que toutes les credentials sont présentes
        if not all([client_key, client_secret, access_token]):
            raise ValueError("Les credentials Mastodon sont manquantes dans le .env")
        
        # Créer le client Mastodon
        self.client = Mastodon(
            client_id=client_key,
            client_secret=client_secret,
            access_token=access_token,
            api_base_url=self.instance_url
        )
        print(f" Connecté à {self.instance_url}")
    
    def get_client(self) -> Mastodon:
        """Retourne le client Mastodon configuré"""
        return self.client
    
    def stream_public_timeline(self, handler, reconnect_async: bool = True, max_retries: int = 10):
        """
        Récupère les posts via polling sur le hashtag #news.
        timeline_public retourne 0 posts (rate limiting ou restriction).
        
        Args:
            handler: Objet StreamHandler avec les méthodes de callback
            reconnect_async: Non utilisé (compatibilité)
            max_retries: Non utilisé (compatibilité)
        """
        if not self.client:
            raise RuntimeError("Client Mastodon non initialisé")
        
        print("🔄 Démarrage du polling Mastodon (hashtag #news)...", flush=True)
        print("📊 Récupération des nouveaux posts toutes les 3 secondes...", flush=True)
        
        last_id = None
        
        while True:
            try:
                # Utiliser timeline_hashtag car timeline_public retourne 0 posts
                if last_id:
                    posts = self.client.timeline_hashtag('news', since_id=last_id, limit=40)
                else:
                    posts = self.client.timeline_hashtag('news', limit=20)
                
                # Traiter les posts (du plus ancien au plus récent)
                for post in reversed(posts):
                    handler.on_update(post)
                    last_id = post['id']
                
                # Attendre avant la prochaine requête
                time.sleep(3)
                
            except KeyboardInterrupt:
                print(f"\n⏹️ Arrêt du polling | Total posts traités: {handler.messages_count}", flush=True)
                break
            except Exception as e:
                print(f"⚠️ Erreur polling: {e}", flush=True)
                time.sleep(5)
