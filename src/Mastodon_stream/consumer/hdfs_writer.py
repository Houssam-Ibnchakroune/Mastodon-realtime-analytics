import pydoop.hdfs as hdfs
import json
import logging
from typing import Any, Dict

logger = logging.getLogger(__name__)


class HDFSWriter:
    """
    Utility class for writing data to HDFS.
    """
    
    def __init__(self, hdfs_path: str):
        """
        Initialize HDFS writer.
        
        Args:
            hdfs_path: Full HDFS path (e.g., 'hdfs://localhost:9000/kafka_demo/tweets_data.json')
        """
        self.hdfs_path = hdfs_path
        self._ensure_file_exists()
        logger.info(f"HDFS writer initialized for path: {hdfs_path}")
    
    def _ensure_file_exists(self):
        """Create the file if it doesn't exist."""
        try:
            if not hdfs.path.exists(self.hdfs_path):
                # Create empty file
                with hdfs.open(self.hdfs_path, 'wt') as f:
                    pass  # Just create the file
                logger.info(f"Created HDFS file: {self.hdfs_path}")
        except Exception as e:
            logger.error(f"Failed to create HDFS file: {e}")
            raise
    
    def write_message(self, message: Dict[str, Any], append: bool = True):
        """
        Write a single message to HDFS.
        
        Args:
            message: Message data to write (will be JSON serialized)
            append: If True, append to file; if False, overwrite
        """
        mode = 'at' if append else 'wt'
        
        try:
            with hdfs.open(self.hdfs_path, mode) as file:
                # Convert message to JSON string and write
                json_str = json.dumps(message, ensure_ascii=False)
                file.write(f"{json_str}\n")
                logger.debug(f"Message written to HDFS: {self.hdfs_path}")
                
        except Exception as e:
            logger.error(f"Failed to write to HDFS: {e}")
            raise
    
    def write_batch(self, messages: list[Dict[str, Any]], append: bool = True):
        """
        Write multiple messages to HDFS in a batch.
        
        Args:
            messages: List of messages to write
            append: If True, append to file; if False, overwrite
        """
        mode = 'at' if append else 'wt'
        
        try:
            with hdfs.open(self.hdfs_path, mode) as file:
                for message in messages:
                    json_str = json.dumps(message, ensure_ascii=False)
                    file.write(f"{json_str}\n")
                    
                logger.info(f"Batch of {len(messages)} messages written to HDFS")
                
        except Exception as e:
            logger.error(f"Failed to write batch to HDFS: {e}")
            raise
    
    def path_exists(self) -> bool:
        """
        Check if the HDFS path exists.
        
        Returns:
            True if path exists, False otherwise
        """
        try:
            return hdfs.path.exists(self.hdfs_path)
        except Exception as e:
            logger.error(f"Error checking HDFS path: {e}")
            return False
