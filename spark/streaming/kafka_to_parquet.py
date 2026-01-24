"""
Spark Streaming Consumer - Kafka to Parquet Data Lake
Reads from Kafka and writes to Parquet files with checkpointing
Author: Vikas Pabba
"""

import sys
import os
from datetime import datetime
from pyspark.sql.functions import (
    col, from_json, to_timestamp, window, 
    count, avg, sum as spark_sum, round as spark_round,
    current_timestamp, date_format
)

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from spark.utils.spark_config import SparkConfig
from spark.utils.schemas import TaxiSchemas


def create_kafka_stream(spark, kafka_servers: str, topic: str):
    """Create Kafka stream reader"""
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
    """Parse JSON trip data from Kafka"""
    trip_schema = TaxiSchemas.get_trip_schema()
    
    # Parse JSON from Kafka value
    parsed_df = df.select(
        col("key").cast("string").alias("kafka_key"),
        col("timestamp").alias("kafka_timestamp"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        from_json(col("value").cast("string"), trip_schema).alias("trip")
    )
    
    # Flatten the structure
    trip_df = parsed_df.select(
        "kafka_key",
        "kafka_timestamp",
        "kafka_partition",
        "kafka_offset",
        "trip.*"
    )
    
    # Convert timestamp strings to actual timestamps
    trip_df = trip_df \
        .withColumn("pickup_datetime", to_timestamp(col("pickup_datetime"))) \
        .withColumn("dropoff_datetime", to_timestamp(col("dropoff_datetime"))) \
        .withColumn("processing_time", current_timestamp())
    
    # Add date partition columns for efficient querying
    trip_df = trip_df \
        .withColumn("year", date_format(col("pickup_datetime"), "yyyy")) \
        .withColumn("month", date_format(col("pickup_datetime"), "MM")) \
        .withColumn("day", date_format(col("pickup_datetime"), "dd"))
    
    return trip_df


def write_to_parquet(trip_df, output_path: str, checkpoint_path: str):
    """
    Write streaming data to Parquet files with partitioning
    
    Args:
        trip_df: Parsed trip DataFrame
        output_path: Path to write Parquet files
        checkpoint_path: Path for checkpointing
    """
    print(f"\n💾 Writing to Parquet data lake...")
    print(f"📁 Output path: {output_path}")
    print(f"🔖 Checkpoint path: {checkpoint_path}")
    print("=" * 80)
    
    query = trip_df \
        .writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", output_path) \
        .option("checkpointLocation", checkpoint_path) \
        .partitionBy("year", "month", "day", "pickup_borough") \
        .trigger(processingTime="30 seconds") \
        .start()
    
    return query


def write_aggregations_to_parquet(trip_df, output_path: str, checkpoint_path: str):
    """
    Write aggregated metrics to Parquet files
    
    Args:
        trip_df: Parsed trip DataFrame
        output_path: Path to write Parquet files
        checkpoint_path: Path for checkpointing
    """
    print(f"\n📊 Writing aggregated metrics to Parquet...")
    print(f"📁 Output path: {output_path}")
    print(f"🔖 Checkpoint path: {checkpoint_path}")
    print("=" * 80)
    
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
            spark_round(avg("trip_duration_seconds"), 2).alias("avg_trip_duration_sec"),
            current_timestamp().alias("processing_time")
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
            "avg_trip_duration_sec",
            "processing_time",
            date_format(col("window.start"), "yyyy").alias("year"),
            date_format(col("window.start"), "MM").alias("month"),
            date_format(col("window.start"), "dd").alias("day")
        )
    
    query = agg_df \
        .writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", output_path) \
        .option("checkpointLocation", checkpoint_path) \
        .partitionBy("year", "month", "day") \
        .trigger(processingTime="1 minute") \
        .start()
    
    return query


def write_to_console(trip_df, mode: str = "summary"):
    """
    Write monitoring output to console
    
    Args:
        trip_df: Parsed trip DataFrame
        mode: 'summary' or 'detailed'
    """
    print(f"\n📺 Console monitoring ({mode} mode)...")
    print("=" * 80)
    
    if mode == "summary":
        # Show summary statistics every 30 seconds
        summary_df = trip_df \
            .groupBy("pickup_borough") \
            .agg(
                count("trip_id").alias("trips"),
                spark_round(avg("total_amount"), 2).alias("avg_fare")
            )
        
        query = summary_df \
            .writeStream \
            .outputMode("complete") \
            .format("console") \
            .option("truncate", "false") \
            .option("numRows", "10") \
            .trigger(processingTime="30 seconds") \
            .start()
    else:
        # Show detailed trip data
        query = trip_df \
            .select(
                "trip_id",
                "pickup_datetime",
                "pickup_borough",
                "dropoff_borough",
                "trip_distance",
                "total_amount"
            ) \
            .writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", "false") \
            .option("numRows", "5") \
            .start()
    
    return query


def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Spark Streaming - Kafka to Parquet Data Lake')
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
        '--output-base-path',
        default='./data/processed',
        help='Base path for output data (default: ./data/processed)'
    )
    parser.add_argument(
        '--console',
        choices=['summary', 'detailed', 'none'],
        default='summary',
        help='Console output mode (default: summary)'
    )
    
    args = parser.parse_args()
    
    # Setup paths
    trips_output_path = os.path.join(args.output_base_path, "trips_parquet")
    trips_checkpoint_path = os.path.join("checkpoint", "trips_parquet")
    
    agg_output_path = os.path.join(args.output_base_path, "aggregations_parquet")
    agg_checkpoint_path = os.path.join("checkpoint", "aggregations_parquet")
    
    print("\n" + "=" * 80)
    print("🚀 NYC TAXI ANALYTICS - PARQUET DATA LAKE WRITER")
    print("=" * 80)
    print(f"📍 Kafka Servers: {args.kafka_servers}")
    print(f"📋 Topic: {args.topic}")
    print(f"💾 Trips Output: {trips_output_path}")
    print(f"📊 Aggregations Output: {agg_output_path}")
    print(f"📺 Console Mode: {args.console}")
    print("=" * 80 + "\n")
    
    # Create Spark session
    print("⚡ Initializing Spark session...")
    spark = SparkConfig.get_spark_session("Kafka-to-Parquet-DataLake")
    
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
        
        # Write raw trips to Parquet
        trips_query = write_to_parquet(trip_df, trips_output_path, trips_checkpoint_path)
        queries.append(("Raw Trips", trips_query))
        
        # Write aggregations to Parquet
        agg_query = write_aggregations_to_parquet(trip_df, agg_output_path, agg_checkpoint_path)
        queries.append(("Aggregations", agg_query))
        
        # Optional console output
        if args.console != 'none':
            console_query = write_to_console(trip_df, args.console)
            queries.append(("Console", console_query))
        
        print("\n" + "=" * 80)
        print("✅ ALL STREAMING QUERIES STARTED!")
        print("=" * 80)
        print(f"📝 Active Queries: {len(queries)}")
        for name, _ in queries:
            print(f"   ✓ {name}")
        print("=" * 80)
        print("💾 Writing data to Parquet... (Press Ctrl+C to stop)")
        print("🔍 Monitor progress in Spark UI: http://localhost:4040")
        print("=" * 80 + "\n")
        
        # Wait for all queries to finish
        for name, query in queries:
            query.awaitTermination()
            
    except KeyboardInterrupt:
        print("\n\n⚠️  Stopping streaming queries...")
        for name, query in queries:
            print(f"   Stopping {name}...")
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
        
        # Show output summary
        print("\n" + "=" * 80)
        print("📊 DATA LAKE SUMMARY")
        print("=" * 80)
        print(f"Raw Trips: {trips_output_path}")
        print(f"Aggregations: {agg_output_path}")
        print("\n💡 To query the data:")
        print(f"   spark.read.parquet('{trips_output_path}').show()")
        print(f"   spark.read.parquet('{agg_output_path}').show()")
        print("=" * 80 + "\n")


if __name__ == "__main__":
    main()