"""
Spark Configuration Module
Author: Vikas Pabba
"""

import os
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf


class SparkConfig:
    """Spark configuration for streaming and batch jobs"""
    
    @staticmethod
    def get_spark_session(app_name: str = "NYC-Taxi-Analytics", 
                         streaming: bool = True) -> SparkSession:
        """
        Create and configure Spark session
        
        Args:
            app_name: Application name
            streaming: Whether this is for streaming (affects config)
            
        Returns:
            Configured SparkSession
        """
        
        # Base configuration
        conf = SparkConf()
        conf.setAppName(app_name)
        
        # Spark settings
        conf.set("spark.sql.shuffle.partitions", "12")
        conf.set("spark.default.parallelism", "12")
        
        # Memory settings
        conf.set("spark.driver.memory", "4g")
        conf.set("spark.executor.memory", "4g")
        conf.set("spark.driver.maxResultSize", "2g")
        
        # Streaming-specific settings
        if streaming:
            conf.set("spark.streaming.stopGracefullyOnShutdown", "true")
            conf.set("spark.sql.streaming.schemaInference", "true")
            conf.set("spark.sql.adaptive.enabled", "false")
        
        # Kafka settings
        conf.set("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
        
        # Performance tuning
        conf.set("spark.sql.files.maxPartitionBytes", "134217728")
        conf.set("spark.sql.files.openCostInBytes", "134217728")
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        
        # Build session
        builder = SparkSession.builder.config(conf=conf)
        
        # Enable Hive support
        spark = builder.enableHiveSupport().getOrCreate()
        
        # Set log level
        spark.sparkContext.setLogLevel("WARN")
        
        return spark
    
    @staticmethod
    def get_checkpoint_location(job_name: str) -> str:
        """Get checkpoint location for streaming job"""
        base_dir = os.path.join(os.getcwd(), "checkpoint")
        checkpoint_dir = os.path.join(base_dir, job_name)
        os.makedirs(checkpoint_dir, exist_ok=True)
        return checkpoint_dir
    
    @staticmethod
    def get_output_location(output_type: str) -> str:
        """Get output location for data"""
        base_dir = os.path.join(os.getcwd(), "data", "processed")
        output_dir = os.path.join(base_dir, output_type)
        os.makedirs(output_dir, exist_ok=True)
        return output_dir


if __name__ == "__main__":
    print("Testing Spark Configuration...")
    spark = SparkConfig.get_spark_session("TestApp")
    print(f"Spark Version: {spark.version}")
    print(f"Spark App Name: {spark.sparkContext.appName}")
    print(f"Spark Master: {spark.sparkContext.master}")
    checkpoint = SparkConfig.get_checkpoint_location("test_job")
    print(f"Checkpoint Location: {checkpoint}")
    output = SparkConfig.get_output_location("test_output")
    print(f"Output Location: {output}")
    print("\nKey Spark Configurations:")
    for conf in spark.sparkContext.getConf().getAll():
        if 'spark.sql' in conf[0] or 'spark.streaming' in conf[0]:
            print(f"  {conf[0]}: {conf[1]}")
    spark.stop()
    print("\n✅ Spark configuration test completed!")