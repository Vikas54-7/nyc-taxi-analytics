"""
Test script for Kafka producer
Author: Vikas Pabba
"""

import sys
import time
from confluent_kafka import Consumer, KafkaException

# Add parent directory to path for imports
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from kafka.config.kafka_config import kafka_config


def test_consumer():
    """Test consumer to verify messages are being produced"""
    
    consumer_conf = kafka_config.get_consumer_config()
    consumer_conf['group.id'] = 'test-consumer-group'
    consumer = Consumer(consumer_conf)
    
    print(f"Subscribing to topic: {kafka_config.topic_name}")
    consumer.subscribe([kafka_config.topic_name])
    
    print("\nWaiting for messages... (Press Ctrl+C to stop)\n")
    
    message_count = 0
    
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            
            if msg is None:
                continue
            
            if msg.error():
                raise KafkaException(msg.error())
            
            message_count += 1
            
            # Parse and display message
            key = msg.key().decode('utf-8') if msg.key() else None
            value = msg.value().decode('utf-8')
            
            print(f"Message {message_count}:")
            print(f"  Topic: {msg.topic()}")
            print(f"  Partition: {msg.partition()}")
            print(f"  Offset: {msg.offset()}")
            print(f"  Key: {key}")
            print(f"  Value (first 200 chars): {value[:200]}...")
            print(f"  Timestamp: {msg.timestamp()}")
            print("-" * 80)
            
            # Commit offset
            consumer.commit(asynchronous=False)
            
    except KeyboardInterrupt:
        print(f"\nConsumed {message_count} messages")
    finally:
        consumer.close()


if __name__ == "__main__":
    test_consumer()