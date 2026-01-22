"""
Kafka Configuration Module
Author: Vikas Pabba
"""

import os
from dataclasses import dataclass
from typing import Dict, Any
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


@dataclass
class KafkaConfig:
    """Kafka configuration settings"""
    
    # Broker Configuration
    bootstrap_servers: str = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    
    # Topic Configuration
    topic_name: str = os.getenv('KAFKA_TOPIC_TAXI_TRIPS', 'taxi-trips')
    num_partitions: int = 12
    replication_factor: int = 1
    
    # Producer Configuration
    producer_config: Dict[str, Any] = None
    
    # Consumer Configuration
    consumer_config: Dict[str, Any] = None
    
    def __post_init__(self):
        """Initialize producer and consumer configurations"""
        
        # Producer settings for high throughput (confluent-kafka compatible)
        self.producer_config = {
            'bootstrap.servers': self.bootstrap_servers,
            'client.id': 'nyc-taxi-producer',
            'acks': 'all',  # Wait for all replicas
            'retries': 3,
            'compression.type': 'snappy',
            'linger.ms': 10,  # Batch messages for 10ms
            'batch.num.messages': 10000,  # Batch size in messages
            'queue.buffering.max.messages': 100000,  # Max messages in queue
            'queue.buffering.max.kbytes': 65536,  # 64MB buffer
            'enable.idempotence': True,  # Exactly-once semantics
        }
        
        # Consumer settings (confluent-kafka compatible)
        self.consumer_config = {
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': 'taxi-analytics-consumer-group',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,  # Manual commit for reliability
            'session.timeout.ms': 30000,
            'heartbeat.interval.ms': 10000,
        }
    
    def get_producer_config(self) -> Dict[str, Any]:
        """Get producer configuration"""
        return self.producer_config
    
    def get_consumer_config(self) -> Dict[str, Any]:
        """Get consumer configuration"""
        return self.consumer_config
    
    @staticmethod
    def get_topic_config() -> Dict[str, str]:
        """Get topic configuration for creation"""
        return {
            'cleanup.policy': 'delete',
            'retention.ms': '604800000',  # 7 days
            'segment.ms': '86400000',  # 1 day
            'compression.type': 'snappy',
            'max.message.bytes': '1048576',  # 1MB
        }


# Global configuration instance
kafka_config = KafkaConfig()


if __name__ == "__main__":
    # Test configuration
    print("Kafka Configuration:")
    print(f"Bootstrap Servers: {kafka_config.bootstrap_servers}")
    print(f"Topic Name: {kafka_config.topic_name}")
    print(f"\nProducer Config:")
    for key, value in kafka_config.get_producer_config().items():
        print(f"  {key}: {value}")
    print(f"\nConsumer Config:")
    for key, value in kafka_config.get_consumer_config().items():
        print(f"  {key}: {value}")