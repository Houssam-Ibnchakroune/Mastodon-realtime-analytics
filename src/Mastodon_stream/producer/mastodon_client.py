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
        Lance le stream de la timeline publique avec auto-reconnexion
        
        Args:
            handler: Objet StreamHandler avec les méthodes de callback
            reconnect_async: Activer la reconnexion automatique
            max_retries: Nombre maximum de tentatives de reconnexion
        """
        if not self.client:
            raise RuntimeError("Client Mastodon non initialisé")
        
        print("🔄 Démarrage du stream Mastodon...")
        
        retries = 0
        while retries < max_retries:
            try:
                # reconnect_async=True permet au stream de se reconnecter automatiquement
                self.client.stream_public(handler, reconnect_async=reconnect_async)
                break  # Si le stream se termine normalement, on sort
            except Exception as e:
                retries += 1
                logger.warning(f"⚠️ Stream interrompu ({retries}/{max_retries}): {e}")
                if retries < max_retries:
                    wait_time = min(30, 2 ** retries)  # Exponential backoff, max 30s
                    logger.info(f"🔄 Reconnexion dans {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    logger.error("❌ Nombre maximum de tentatives atteint")
                    raise
