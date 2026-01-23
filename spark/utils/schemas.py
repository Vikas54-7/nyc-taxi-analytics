"""
Data Schemas for Spark Processing
Author: Vikas Pabba
"""

from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, 
    IntegerType, TimestampType, LongType
)


class TaxiSchemas:
    """Schema definitions for taxi trip data"""
    
    @staticmethod
    def get_trip_schema() -> StructType:
        """
        Get schema for taxi trip records from Kafka
        
        Returns:
            StructType schema for trip data
        """
        return StructType([
            StructField("trip_id", StringType(), False),
            StructField("vendor_id", StringType(), True),
            StructField("pickup_datetime", StringType(), True),
            StructField("dropoff_datetime", StringType(), True),
            StructField("passenger_count", IntegerType(), True),
            StructField("trip_distance", DoubleType(), True),
            StructField("trip_duration_seconds", LongType(), True),
            StructField("pickup_longitude", DoubleType(), True),
            StructField("pickup_latitude", DoubleType(), True),
            StructField("pickup_borough", StringType(), True),
            StructField("dropoff_longitude", DoubleType(), True),
            StructField("dropoff_latitude", DoubleType(), True),
            StructField("dropoff_borough", StringType(), True),
            StructField("fare_amount", DoubleType(), True),
            StructField("extra", DoubleType(), True),
            StructField("mta_tax", DoubleType(), True),
            StructField("tip_amount", DoubleType(), True),
            StructField("tolls_amount", DoubleType(), True),
            StructField("improvement_surcharge", DoubleType(), True),
            StructField("total_amount", DoubleType(), True),
            StructField("payment_type", StringType(), True),
            StructField("trip_type", StringType(), True),
            StructField("pickup_zone", StringType(), True),
            StructField("rate_code", StringType(), True),
            StructField("store_and_fwd_flag", StringType(), True),
        ])
    
    @staticmethod
    def get_aggregated_schema() -> StructType:
        """
        Get schema for aggregated trip metrics
        
        Returns:
            StructType schema for aggregated data
        """
        return StructType([
            StructField("window_start", TimestampType(), False),
            StructField("window_end", TimestampType(), False),
            StructField("pickup_borough", StringType(), True),
            StructField("trip_count", LongType(), False),
            StructField("total_passengers", LongType(), False),
            StructField("avg_trip_distance", DoubleType(), True),
            StructField("avg_fare_amount", DoubleType(), True),
            StructField("total_revenue", DoubleType(), True),
            StructField("avg_trip_duration", DoubleType(), True),
        ])
    
    @staticmethod
    def print_schema_info():
        """Print schema information for documentation"""
        print("=" * 80)
        print("TAXI TRIP SCHEMA")
        print("=" * 80)
        
        schema = TaxiSchemas.get_trip_schema()
        
        print(f"\nTotal Fields: {len(schema.fields)}\n")
        
        for field in schema.fields:
            nullable = "NULL" if field.nullable else "NOT NULL"
            print(f"{field.name:30} {str(field.dataType):20} {nullable}")
        
        print("\n" + "=" * 80)
        print("AGGREGATED METRICS SCHEMA")
        print("=" * 80)
        
        agg_schema = TaxiSchemas.get_aggregated_schema()
        
        print(f"\nTotal Fields: {len(agg_schema.fields)}\n")
        
        for field in agg_schema.fields:
            nullable = "NULL" if field.nullable else "NOT NULL"
            print(f"{field.name:30} {str(field.dataType):20} {nullable}")
        
        print("\n" + "=" * 80)


if __name__ == "__main__":
    # Print schema documentation
    TaxiSchemas.print_schema_info()