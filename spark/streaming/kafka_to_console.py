"""
Spark Streaming Consumer - Kafka to Console
Reads from Kafka and displays data in real-time
Author: Vikas Pabba
"""

import sys
import os
from pyspark.sql.functions import (
    col, from_json, to_timestamp, window, 
    count, avg, sum as spark_sum, round as spark_round
)

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from spark.utils.spark_config import SparkConfig
from spark.utils.schemas import TaxiSchemas


def create_kafka_stream(spark, kafka_servers: str, topic: str):
    """
    Create Kafka stream reader
    
    Args:
        spark: SparkSession
        kafka_servers: Kafka bootstrap servers
        topic: Kafka topic name
        
    Returns:
        Streaming DataFrame
    """
    print(f"📡 Connecting to Kafka at {kafka_servers}")
    print(f"📋 Subscribing to topic: {topic}")
    
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_servers) \
        .option("subscribe", topic) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()
    
    print("✅ Kafka stream created successfully!")
    return df


def parse_trip_data(df):
    """
    Parse JSON trip data from Kafka
    
    Args:
        df: Raw Kafka DataFrame
        
    Returns:
        Parsed DataFrame with trip data
    """
    trip_schema = TaxiSchemas.get_trip_schema()
    
    # Parse JSON from Kafka value
    parsed_df = df.select(
        col("key").cast("string").alias("kafka_key"),
        col("timestamp").alias("kafka_timestamp"),
        from_json(col("value").cast("string"), trip_schema).alias("trip")
    )
    
    # Flatten the structure
    trip_df = parsed_df.select(
        "kafka_key",
        "kafka_timestamp",
        "trip.*"
    )
    
    # Convert timestamp strings to actual timestamps
    trip_df = trip_df \
        .withColumn("pickup_datetime", to_timestamp(col("pickup_datetime"))) \
        .withColumn("dropoff_datetime", to_timestamp(col("dropoff_datetime")))
    
    return trip_df


def display_raw_stream(trip_df):
    """
    Display raw trip data to console
    
    Args:
        trip_df: Parsed trip DataFrame
    """
    print("\n🚖 Starting raw trip data stream to console...")
    print("=" * 80)
    
    query = trip_df \
        .select(
            "trip_id",
            "pickup_datetime",
            "pickup_borough",
            "dropoff_borough",
            "trip_distance",
            "fare_amount",
            "total_amount",
            "payment_type"
        ) \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .option("numRows", "10") \
        .start()
    
    return query


def calculate_aggregations(trip_df):
    """
    Calculate streaming aggregations
    
    Args:
        trip_df: Parsed trip DataFrame
        
    Returns:
        Aggregated DataFrame
    """
    # 1-minute tumbling window aggregations by borough
    agg_df = trip_df \
        .withWatermark("pickup_datetime", "2 minutes") \
        .groupBy(
            window(col("pickup_datetime"), "1 minute"),
            col("pickup_borough")
        ) \
        .agg(
            count("trip_id").alias("trip_count"),
            spark_sum("passenger_count").alias("total_passengers"),
            spark_round(avg("trip_distance"), 2).alias("avg_trip_distance"),
            spark_round(avg("fare_amount"), 2).alias("avg_fare_amount"),
            spark_round(spark_sum("total_amount"), 2).alias("total_revenue"),
            spark_round(avg("trip_duration_seconds"), 2).alias("avg_trip_duration_sec")
        ) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "pickup_borough",
            "trip_count",
            "total_passengers",
            "avg_trip_distance",
            "avg_fare_amount",
            "total_revenue",
            "avg_trip_duration_sec"
        ) \
        .orderBy("window_start", "pickup_borough")
    
    return agg_df


def display_aggregations(agg_df):
    """
    Display aggregated metrics to console
    
    Args:
        agg_df: Aggregated DataFrame
    """
    print("\n📊 Starting aggregated metrics stream to console...")
    print("=" * 80)
    
    query = agg_df \
        .writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", "false") \
        .option("numRows", "20") \
        .start()
    
    return query


def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Spark Streaming - Kafka to Console')
    parser.add_argument(
        '--kafka-servers',
        default='localhost:9092',
        help='Kafka bootstrap servers (default: localhost:9092)'
    )
    parser.add_argument(
        '--topic',
        default='taxi-trips',
        help='Kafka topic name (default: taxi-trips)'
    )
    parser.add_argument(
        '--mode',
        choices=['raw', 'aggregated', 'both'],
        default='both',
        help='Display mode (default: both)'
    )
    
    args = parser.parse_args()
    
    print("\n" + "=" * 80)
    print("🚀 NYC TAXI ANALYTICS - SPARK STREAMING CONSUMER")
    print("=" * 80)
    print(f"📍 Kafka Servers: {args.kafka_servers}")
    print(f"📋 Topic: {args.topic}")
    print(f"🎯 Mode: {args.mode}")
    print("=" * 80 + "\n")
    
    # Create Spark session
    print("⚡ Initializing Spark session...")
    spark = SparkConfig.get_spark_session("Kafka-to-Console-Streaming")
    
    print(f"✅ Spark version: {spark.version}")
    print(f"✅ App name: {spark.sparkContext.appName}\n")
    
    try:
        # Create Kafka stream
        kafka_df = create_kafka_stream(spark, args.kafka_servers, args.topic)
        
        # Parse trip data
        print("🔧 Parsing trip data...")
        trip_df = parse_trip_data(kafka_df)
        print("✅ Trip data parsed successfully!\n")
        
        queries = []
        
        # Start raw data stream
        if args.mode in ['raw', 'both']:
            raw_query = display_raw_stream(trip_df)
            queries.append(raw_query)
        
        # Start aggregated stream
        if args.mode in ['aggregated', 'both']:
            print("🔧 Calculating aggregations...")
            agg_df = calculate_aggregations(trip_df)
            print("✅ Aggregations configured!\n")
            
            agg_query = display_aggregations(agg_df)
            queries.append(agg_query)
        
        print("\n" + "=" * 80)
        print("✅ ALL STREAMING QUERIES STARTED!")
        print("=" * 80)
        print("📺 Watching for data... (Press Ctrl+C to stop)")
        print("=" * 80 + "\n")
        
        # Wait for all queries to finish
        for query in queries:
            query.awaitTermination()
            
    except KeyboardInterrupt:
        print("\n\n⚠️  Stopping streaming queries...")
        for query in queries:
            query.stop()
        print("✅ All queries stopped gracefully!")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        print("\n🛑 Stopping Spark session...")
        spark.stop()
        print("✅ Spark session stopped!")
        print("\n" + "=" * 80)
        print("👋 Streaming consumer terminated!")
        print("=" * 80 + "\n")


if __name__ == "__main__":
    main()