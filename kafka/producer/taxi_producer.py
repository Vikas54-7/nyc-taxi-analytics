"""
NYC Taxi Kafka Producer
Streams taxi trip data to Kafka topic
Author: Vikas Pabba
"""

import json
import time
import sys
import signal
from datetime import datetime
from typing import Optional
from confluent_kafka import Producer, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
import logging

# Add parent directory to path for imports
sys.path.append('..')
from kafka.config.kafka_config import kafka_config
from kafka.producer.data_generator import NYCTaxiDataGenerator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class TaxiProducer:
    """Kafka producer for NYC taxi trip data"""
    
    def __init__(self):
        """Initialize the producer"""
        self.config = kafka_config
        self.producer: Optional[Producer] = None
        self.data_generator = NYCTaxiDataGenerator()
        self.running = True
        self.messages_sent = 0
        self.messages_failed = 0
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        logger.info("Shutdown signal received. Stopping producer...")
        self.running = False
    
    def _delivery_callback(self, err, msg):
        """Callback for message delivery reports"""
        if err:
            logger.error(f"Message delivery failed: {err}")
            self.messages_failed += 1
        else:
            self.messages_sent += 1
            if self.messages_sent % 100 == 0:
                logger.info(
                    f"Delivered message to {msg.topic()} "
                    f"[partition {msg.partition()}] at offset {msg.offset()}"
                )
    
    def create_topic(self):
        """Create Kafka topic if it doesn't exist"""
        admin_client = AdminClient({
            'bootstrap.servers': self.config.bootstrap_servers
        })
        
        topic_metadata = admin_client.list_topics(timeout=10)
        
        if self.config.topic_name not in topic_metadata.topics:
            logger.info(f"Creating topic: {self.config.topic_name}")
            
            new_topic = NewTopic(
                topic=self.config.topic_name,
                num_partitions=self.config.num_partitions,
                replication_factor=self.config.replication_factor,
                config=kafka_config.get_topic_config()
            )
            
            try:
                futures = admin_client.create_topics([new_topic])
                
                # Wait for topic creation
                for topic, future in futures.items():
                    try:
                        future.result()
                        logger.info(f"Topic {topic} created successfully")
                    except Exception as e:
                        logger.error(f"Failed to create topic {topic}: {e}")
            except Exception as e:
                logger.error(f"Error creating topic: {e}")
        else:
            logger.info(f"Topic {self.config.topic_name} already exists")
    
    def connect(self):
        """Connect to Kafka"""
        try:
            logger.info(f"Connecting to Kafka at {self.config.bootstrap_servers}")
            self.producer = Producer(self.config.get_producer_config())
            logger.info("Successfully connected to Kafka")
            
            # Create topic if needed
            self.create_topic()
            
        except KafkaException as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            raise
    
    def send_message(self, key: str, value: dict):
        """Send a message to Kafka"""
        try:
            # Serialize the message
            message_value = json.dumps(value).encode('utf-8')
            message_key = key.encode('utf-8')
            
            # Produce message
            self.producer.produce(
                topic=self.config.topic_name,
                key=message_key,
                value=message_value,
                callback=self._delivery_callback
            )
            
            # Poll to handle delivery reports
            self.producer.poll(0)
            
        except Exception as e:
            logger.error(f"Error sending message: {e}")
            self.messages_failed += 1
    
    def start_streaming(self, messages_per_second: int = 10, duration_seconds: int = None):
        """
        Start streaming taxi trip data
        
        Args:
            messages_per_second: Number of messages to send per second
            duration_seconds: Duration to run (None for infinite)
        """
        logger.info(f"Starting streaming at {messages_per_second} messages/second")
        
        if duration_seconds:
            logger.info(f"Will run for {duration_seconds} seconds")
        else:
            logger.info("Running indefinitely (Ctrl+C to stop)")
        
        start_time = time.time()
        message_interval = 1.0 / messages_per_second
        
        try:
            while self.running:
                # Check duration
                if duration_seconds and (time.time() - start_time) >= duration_seconds:
                    logger.info("Duration reached. Stopping...")
                    break
                
                # Generate and send trip
                trip = self.data_generator.generate_trip()
                self.send_message(trip['trip_id'], trip)
                
                # Control rate
                time.sleep(message_interval)
                
        except KeyboardInterrupt:
            logger.info("Interrupted by user")
        finally:
            self.shutdown()
    
    def start_batch_streaming(self, batch_size: int = 100, batches: int = None, delay: float = 1.0):
        """
        Start streaming in batches
        
        Args:
            batch_size: Number of messages per batch
            batches: Number of batches to send (None for infinite)
            delay: Delay between batches in seconds
        """
        logger.info(f"Starting batch streaming: {batch_size} messages/batch")
        
        batch_count = 0
        
        try:
            while self.running:
                # Check batch limit
                if batches and batch_count >= batches:
                    logger.info(f"Sent {batch_count} batches. Stopping...")
                    break
                
                # Generate batch
                trips = self.data_generator.generate_batch(batch_size)
                
                # Send each trip
                for trip in trips:
                    self.send_message(trip['trip_id'], trip)
                
                # Flush messages
                self.producer.flush()
                
                batch_count += 1
                logger.info(f"Sent batch {batch_count} ({batch_size} messages)")
                
                # Wait before next batch
                time.sleep(delay)
                
        except KeyboardInterrupt:
            logger.info("Interrupted by user")
        finally:
            self.shutdown()
    
    def shutdown(self):
        """Gracefully shutdown the producer"""
        logger.info("Shutting down producer...")
        
        if self.producer:
            # Wait for messages to be delivered
            logger.info("Flushing remaining messages...")
            remaining = self.producer.flush(timeout=30)
            
            if remaining > 0:
                logger.warning(f"{remaining} messages were not delivered")
        
        # Print statistics
        logger.info(f"\n{'='*60}")
        logger.info("Producer Statistics:")
        logger.info(f"Messages sent successfully: {self.messages_sent}")
        logger.info(f"Messages failed: {self.messages_failed}")
        logger.info(f"Success rate: {(self.messages_sent / (self.messages_sent + self.messages_failed) * 100):.2f}%")
        logger.info(f"{'='*60}\n")


def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='NYC Taxi Kafka Producer')
    parser.add_argument(
        '--mode',
        choices=['stream', 'batch'],
        default='stream',
        help='Streaming mode (default: stream)'
    )
    parser.add_argument(
        '--rate',
        type=int,
        default=10,
        help='Messages per second for stream mode (default: 10)'
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        default=100,
        help='Batch size for batch mode (default: 100)'
    )
    parser.add_argument(
        '--batches',
        type=int,
        default=None,
        help='Number of batches to send (default: infinite)'
    )
    parser.add_argument(
        '--duration',
        type=int,
        default=None,
        help='Duration in seconds for stream mode (default: infinite)'
    )
    parser.add_argument(
        '--delay',
        type=float,
        default=1.0,
        help='Delay between batches in seconds (default: 1.0)'
    )
    
    args = parser.parse_args()
    
    # Create and start producer
    producer = TaxiProducer()
    producer.connect()
    
    if args.mode == 'stream':
        producer.start_streaming(
            messages_per_second=args.rate,
            duration_seconds=args.duration
        )
    else:
        producer.start_batch_streaming(
            batch_size=args.batch_size,
            batches=args.batches,
            delay=args.delay
        )


if __name__ == "__main__":
    main()

