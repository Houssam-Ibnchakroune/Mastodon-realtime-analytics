"""
Elasticsearch writer for storing Flink analysis results.

Stores engagement scores and hashtag analysis from Kafka topics
into Elasticsearch indices for search and visualization.
"""
import json
import logging
from datetime import datetime
from typing import Any, Dict, Optional
from elasticsearch import Elasticsearch

logger = logging.getLogger(__name__)


class ElasticsearchWriter:
    """
    Utility class for writing Flink analysis results to Elasticsearch.
    
    Indices created:
    - mastodon-engagement: Posts enriched with engagement scores
    - mastodon-hashtags: Extracted hashtags from posts
    """
    
    def __init__(
        self,
        hosts: str = "http://localhost:9200",
        engagement_index: str = "mastodon-engagement",
        hashtags_index: str = "mastodon-hashtags",
    ):
        """
        Initialize Elasticsearch writer.
        
        Args:
            hosts: Elasticsearch connection URL
            engagement_index: Index name for engagement data
            hashtags_index: Index name for hashtags data
        """
        self.es = Elasticsearch(hosts)
        self.engagement_index = engagement_index
        self.hashtags_index = hashtags_index
        
        # Verify connection
        if not self.es.ping():
            raise ConnectionError(f"Cannot connect to Elasticsearch at {hosts}")
        
        logger.info(f"Connected to Elasticsearch at {hosts}")
        
        # Create indices with mappings
        self._create_indices()
    
    def _create_indices(self):
        """Create Elasticsearch indices with proper mappings if they don't exist."""
        
        # Engagement index mapping
        engagement_mapping = {
            "mappings": {
                "properties": {
                    "id": {"type": "keyword"},
                    "content": {"type": "text", "analyzer": "standard"},
                    "language": {"type": "keyword"},
                    "username": {"type": "keyword"},
                    "followers_count": {"type": "integer"},
                    "favourites_count": {"type": "integer"},
                    "reblogs_count": {"type": "integer"},
                    "engagement_score": {"type": "integer"},
                    "tags": {"type": "keyword"},
                    "created_at": {"type": "date"},
                    "indexed_at": {"type": "date"},
                }
            }
        }
        
        # Hashtags index mapping
        hashtags_mapping = {
            "mappings": {
                "properties": {
                    "post_id": {"type": "keyword"},
                    "hashtags": {"type": "keyword"},
                    "hashtag_count": {"type": "integer"},
                    "language": {"type": "keyword"},
                    "indexed_at": {"type": "date"},
                }
            }
        }
        
        # Create engagement index
        if not self.es.indices.exists(index=self.engagement_index):
            self.es.indices.create(index=self.engagement_index, body=engagement_mapping)
            logger.info(f"Created index: {self.engagement_index}")
        else:
            logger.info(f"Index already exists: {self.engagement_index}")
        
        # Create hashtags index
        if not self.es.indices.exists(index=self.hashtags_index):
            self.es.indices.create(index=self.hashtags_index, body=hashtags_mapping)
            logger.info(f"Created index: {self.hashtags_index}")
        else:
            logger.info(f"Index already exists: {self.hashtags_index}")
    
    def index_engagement(self, data: Dict[str, Any]) -> bool:
        """
        Index an engagement record into Elasticsearch.
        
        Args:
            data: Engagement data dict from Flink analysis
            
        Returns:
            True if indexed successfully
        """
        try:
            # Add indexing timestamp
            data["indexed_at"] = datetime.utcnow().isoformat()
            
            # Use post ID as document ID to avoid duplicates
            doc_id = data.get("id", None)
            
            self.es.index(
                index=self.engagement_index,
                id=doc_id,
                document=data,
            )
            logger.debug(f"Indexed engagement for post {doc_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to index engagement data: {e}")
            return False
    
    def index_hashtags(self, data: Dict[str, Any]) -> bool:
        """
        Index a hashtag record into Elasticsearch.
        
        Args:
            data: Hashtag data dict from Flink analysis
            
        Returns:
            True if indexed successfully
        """
        try:
            data["indexed_at"] = datetime.utcnow().isoformat()
            
            doc_id = data.get("post_id", None)
            
            self.es.index(
                index=self.hashtags_index,
                id=doc_id,
                document=data,
            )
            logger.debug(f"Indexed hashtags for post {doc_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to index hashtag data: {e}")
            return False
    
    def get_stats(self) -> Dict[str, int]:
        """Get document counts for all indices."""
        stats = {}
        
        for index_name in [self.engagement_index, self.hashtags_index]:
            try:
                if self.es.indices.exists(index=index_name):
                    count = self.es.count(index=index_name)["count"]
                    stats[index_name] = count
                else:
                    stats[index_name] = 0
            except Exception:
                stats[index_name] = -1
        
        return stats
    
    def close(self):
        """Close the Elasticsearch connection."""
        self.es.close()
        logger.info("Elasticsearch connection closed")
