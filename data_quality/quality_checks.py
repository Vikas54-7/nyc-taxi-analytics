"""
Data Quality Checks for NYC Taxi Data
Validates data quality and generates metrics
Author: Vikas Pabba
"""

import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, when, isnan, isnull, 
    avg, min as spark_min, max as spark_max,
    round as spark_round, current_timestamp
)

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from spark.utils.spark_config import SparkConfig


class DataQualityChecker:
    """Data quality validation for taxi trip data"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.quality_metrics = {}
    
    def check_completeness(self, df):
        """
        Check for missing/null values in critical fields
        
        Returns:
            DataFrame with completeness metrics
        """
        print("\n🔍 Checking Data Completeness...")
        print("=" * 80)
        
        total_records = df.count()
        
        critical_fields = [
            'trip_id', 'pickup_datetime', 'dropoff_datetime',
            'pickup_latitude', 'pickup_longitude',
            'dropoff_latitude', 'dropoff_longitude',
            'fare_amount', 'total_amount'
        ]
        
        completeness_checks = []
        
        for field in critical_fields:
            null_count = df.filter(col(field).isNull()).count()
            completeness_pct = ((total_records - null_count) / total_records * 100) if total_records > 0 else 0
            
            completeness_checks.append({
                'field': field,
                'total_records': total_records,
                'null_count': null_count,
                'valid_count': total_records - null_count,
                'completeness_pct': round(completeness_pct, 2)
            })
            
            status = "✅" if completeness_pct >= 98 else "⚠️" if completeness_pct >= 95 else "❌"
            print(f"{status} {field:25} {completeness_pct:6.2f}% complete ({null_count} nulls)")
        
        self.quality_metrics['completeness'] = completeness_checks
        return completeness_checks
    
    def check_validity(self, df):
        """
        Check for invalid values (business rule violations)
        
        Returns:
            DataFrame with validity metrics
        """
        print("\n🎯 Checking Data Validity...")
        print("=" * 80)
        
        total_records = df.count()
        
        validity_checks = []
        
        # Check 1: Fare amount should be positive
        invalid_fares = df.filter((col('fare_amount') < 0) | (col('fare_amount') > 1000)).count()
        validity_checks.append(self._create_validity_metric(
            'Fare Amount Range',
            total_records,
            invalid_fares,
            'fare_amount should be between $0 and $1000'
        ))
        
        # Check 2: Trip distance should be positive
        invalid_distance = df.filter((col('trip_distance') <= 0) | (col('trip_distance') > 100)).count()
        validity_checks.append(self._create_validity_metric(
            'Trip Distance Range',
            total_records,
            invalid_distance,
            'trip_distance should be between 0 and 100 miles'
        ))
        
        # Check 3: Passenger count should be between 1-6
        invalid_passengers = df.filter(
            (col('passenger_count') < 1) | (col('passenger_count') > 6)
        ).count()
        validity_checks.append(self._create_validity_metric(
            'Passenger Count Range',
            total_records,
            invalid_passengers,
            'passenger_count should be between 1 and 6'
        ))
        
        # Check 4: Pickup datetime should be before dropoff
        invalid_times = df.filter(col('pickup_datetime') >= col('dropoff_datetime')).count()
        validity_checks.append(self._create_validity_metric(
            'Datetime Logic',
            total_records,
            invalid_times,
            'pickup_datetime should be before dropoff_datetime'
        ))
        
        # Check 5: GPS coordinates should be valid (NYC area)
        invalid_coords = df.filter(
            (col('pickup_latitude') < 40.4) | (col('pickup_latitude') > 41.0) |
            (col('pickup_longitude') < -74.3) | (col('pickup_longitude') > -73.7)
        ).count()
        validity_checks.append(self._create_validity_metric(
            'GPS Coordinates',
            total_records,
            invalid_coords,
            'Coordinates should be within NYC area'
        ))
        
        self.quality_metrics['validity'] = validity_checks
        return validity_checks
    
    def check_consistency(self, df):
        """
        Check for data consistency issues
        
        Returns:
            DataFrame with consistency metrics
        """
        print("\n🔄 Checking Data Consistency...")
        print("=" * 80)
        
        total_records = df.count()
        
        consistency_checks = []
        
        # Check 1: Total amount consistency
        # total_amount should equal fare + extra + mta_tax + tip + tolls + surcharge
        df_with_calc = df.withColumn(
            'calculated_total',
            col('fare_amount') + col('extra') + col('mta_tax') + 
            col('tip_amount') + col('tolls_amount') + col('improvement_surcharge')
        )
        
        inconsistent_totals = df_with_calc.filter(
            abs(col('total_amount') - col('calculated_total')) > 0.10
        ).count()
        
        consistency_checks.append(self._create_validity_metric(
            'Total Amount Calculation',
            total_records,
            inconsistent_totals,
            'total_amount should equal sum of components'
        ))
        
        self.quality_metrics['consistency'] = consistency_checks
        return consistency_checks
    
    def _create_validity_metric(self, check_name, total, invalid, rule):
        """Helper to create validity metric"""
        valid = total - invalid
        validity_pct = (valid / total * 100) if total > 0 else 0
        
        status = "✅" if validity_pct >= 98 else "⚠️" if validity_pct >= 95 else "❌"
        print(f"{status} {check_name:30} {validity_pct:6.2f}% valid ({invalid} invalid)")
        
        return {
            'check_name': check_name,
            'total_records': total,
            'invalid_count': invalid,
            'valid_count': valid,
            'validity_pct': round(validity_pct, 2),
            'rule': rule
        }
    
    def generate_quality_report(self, df):
        """
        Generate comprehensive data quality report
        
        Returns:
            Dictionary with all quality metrics
        """
        print("\n" + "=" * 80)
        print("📊 DATA QUALITY REPORT")
        print("=" * 80)
        
        total_records = df.count()
        print(f"\n📈 Total Records Processed: {total_records:,}")
        
        # Run all checks
        self.check_completeness(df)
        self.check_validity(df)
        self.check_consistency(df)
        
        # Calculate overall quality score
        all_checks = []
        for category in ['completeness', 'validity', 'consistency']:
            if category in self.quality_metrics:
                for check in self.quality_metrics[category]:
                    pct_key = 'completeness_pct' if category == 'completeness' else 'validity_pct'
                    all_checks.append(check[pct_key])
        
        overall_score = sum(all_checks) / len(all_checks) if all_checks else 0
        
        print("\n" + "=" * 80)
        print(f"🎯 OVERALL DATA QUALITY SCORE: {overall_score:.2f}%")
        print("=" * 80)
        
        if overall_score >= 98:
            print("✅ EXCELLENT - Data quality meets production standards")
        elif overall_score >= 95:
            print("⚠️  GOOD - Minor quality issues detected")
        else:
            print("❌ POOR - Significant quality issues require attention")
        
        print("=" * 80 + "\n")
        
        return {
            'total_records': total_records,
            'overall_score': round(overall_score, 2),
            'metrics': self.quality_metrics,
            'timestamp': str(current_timestamp())
        }
    
    def get_data_profile(self, df):
        """Get statistical profile of the data"""
        print("\n📊 DATA PROFILE")
        print("=" * 80)
        
        # Numeric field statistics
        numeric_fields = [
            'trip_distance', 'fare_amount', 'total_amount', 
            'passenger_count', 'trip_duration_seconds'
        ]
        
        for field in numeric_fields:
            stats = df.select(
                spark_min(field).alias('min'),
                spark_max(field).alias('max'),
                spark_round(avg(field), 2).alias('avg')
            ).collect()[0]
            
            print(f"{field:25} Min: {stats['min']:10.2f}  Max: {stats['max']:10.2f}  Avg: {stats['avg']:10.2f}")
        
        # Categorical field distributions
        print("\n📊 Borough Distribution:")
        df.groupBy('pickup_borough').count().orderBy(col('count').desc()).show()
        
        print("💳 Payment Type Distribution:")
        df.groupBy('payment_type').count().orderBy(col('count').desc()).show()
        
        print("=" * 80 + "\n")


def main():
    """Main function to run quality checks on Parquet data"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Data Quality Checker')
    parser.add_argument(
        '--input-path',
        default='./data/processed/trips_parquet',
        help='Path to input Parquet files'
    )
    parser.add_argument(
        '--sample-size',
        type=int,
        default=None,
        help='Sample size for quality checks (None = all data)'
    )
    
    args = parser.parse_args()
    
    print("\n" + "=" * 80)
    print("🔍 DATA QUALITY CHECKER")
    print("=" * 80)
    print(f"📁 Input Path: {args.input_path}")
    if args.sample_size:
        print(f"📊 Sample Size: {args.sample_size:,} records")
    print("=" * 80 + "\n")
    
    # Create Spark session
    spark = SparkConfig.get_spark_session("DataQualityChecker", streaming=False)
    
    try:
        # Read Parquet data
        print("📖 Reading Parquet data...")
        df = spark.read.parquet(args.input_path)
        
        if args.sample_size:
            df = df.limit(args.sample_size)
        
        print(f"✅ Loaded {df.count():,} records\n")
        
        # Run quality checks
        checker = DataQualityChecker(spark)
        report = checker.generate_quality_report(df)
        checker.get_data_profile(df)
        
        # Save report (optional)
        report_path = './data/quality_reports'
        os.makedirs(report_path, exist_ok=True)
        
        import json
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        report_file = os.path.join(report_path, f'quality_report_{timestamp}.json')
        
        with open(report_file, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        
        print(f"💾 Quality report saved to: {report_file}\n")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        spark.stop()


if __name__ == "__main__":
    from datetime import datetime
    main()