import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, min, max

# Import hypothesis modules
from hypothesis1 import analyze_winter_precipitation_lag
from hypothesis2 import analyze_spring_summer_heat
from hypothesis3 import analyze_wind_temperature_moderation
from hypothesis4 import analyze_snowmelt_spring_precipitation
from hypothesis5 import analyze_regional_consistency
from utils import check_data_coverage, validate_data_quality, create_mysql_tables_sql
from mysql_export import MySQLExporter, export_all_results


# =============================================================================
# SPARK SESSION INITIALIZATION
# =============================================================================

def create_spark_session(app_name="ClimatePatternAnalysis"):
    """Initialize Spark session with optimized configuration"""
    spark = SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark


# =============================================================================
# DATA LOADING
# =============================================================================

def load_climate_data(spark, filepath):
    """
    Load cleaned NOAA data in long format
    
    Expected Schema: STATION, DATE, ELEMENT, VALUE
    - STATION: Station ID (string)
    - DATE: YYYYMMDD format (integer)
    - ELEMENT: Variable type - TMAX, TMIN, PRCP, SNOW, SNWD, AWND (string)
    - VALUE: Measurement value (double)
    """
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
    
    schema = StructType([
        StructField("STATION", StringType(), False),
        StructField("DATE", IntegerType(), False),
        StructField("ELEMENT", StringType(), False),
        StructField("VALUE", DoubleType(), True)
    ])
    
    df = spark.read.csv(filepath, schema=schema, header=True)
    
    # Add derived date columns
    df = df.withColumn("year", (col("DATE") / 10000).cast("int")) \
           .withColumn("month", ((col("DATE") % 10000) / 100).cast("int")) \
           .withColumn("day", (col("DATE") % 100).cast("int"))
    
    return df


# =============================================================================
# RESULTS SUMMARY
# =============================================================================

def print_summary(h1_results, h2_results, h3_results, h4_results):
    """Print formatted summary of all hypothesis results"""
    
    print("\n" + "="*80)
    print("SUMMARY OF RESULTS")
    print("="*80)
    
    print("\n" + "-"*60)
    print("H1 - Winter Precipitation Compensation")
    print("-"*60)
    print(f"  Correlation (early vs late winter precip): {h1_results['correlation']:.4f}")
    print(f"  Sample size: {h1_results['sample_size']:,} station-years")
    if h1_results.get('compensation_rate'):
        print(f"  Compensation rate: {h1_results['compensation_rate']:.1f}%")
    
    print("\n" + "-"*60)
    print("H2 - Spring Temperature → Summer Heat Waves")
    print("-"*60)
    print(f"  Correlation (spring temp vs heat wave days): {h2_results['correlation_days']:.4f}")
    print(f"  Correlation (spring temp vs max heat intensity): {h2_results['correlation_intensity']:.4f}")
    print(f"  Avg heat wave days after warm spring (>1σ): {h2_results['avg_hw_warm_spring']:.1f}")
    print(f"  Avg heat wave days after cool spring (<-1σ): {h2_results['avg_hw_cool_spring']:.1f}")
    
    print("\n" + "-"*60)
    print("H3 - Wind → Temperature Moderation")
    print("-"*60)
    print(f"  High wind events analyzed: {h3_results['num_events']:,}")
    print(f"  Avg temp range BEFORE high wind: {h3_results['avg_range_before']:.2f}°C")
    print(f"  Avg temp range AFTER high wind: {h3_results['avg_range_after']:.2f}°C")
    print(f"  Average change: {h3_results['avg_range_change']:.2f}°C")
    if h3_results.get('moderation_rate'):
        print(f"  Events showing moderation: {h3_results['moderation_rate']:.1f}%")
    
    print("\n" + "-"*60)
    print("H4 - Snowmelt → Spring Precipitation")
    print("-"*60)
    print(f"  Correlation (melt timing vs spring precip): {h4_results['correlation']:.4f}")
    print(f"  Sample size: {h4_results['sample_size']:,} station-years")
    print(f"  Avg spring precip anomaly after EARLY melt: {h4_results['early_melt_prcp_anomaly']:.2f}σ")
    print(f"  Avg spring precip anomaly after LATE melt: {h4_results['late_melt_prcp_anomaly']:.2f}σ")
    
    print("\n" + "="*80)


def save_results_to_json(h1_results, h2_results, h3_results, h4_results, output_path):
    """Save all results to JSON file"""
    import json
    
    results = {
        "hypothesis_1_winter_precipitation": h1_results,
        "hypothesis_2_spring_summer_heat": h2_results,
        "hypothesis_3_wind_moderation": h3_results,
        "hypothesis_4_snowmelt_precipitation": h4_results
    }
    
    with open(output_path, 'w') as f:
        json.dump(results, f, indent=2, default=str)
    
    print(f"\nResults saved to {output_path}")


# =============================================================================
# MAIN EXECUTION
# =============================================================================

def main(data_path, output_dir="results", run_quality_checks=True,
         mysql_host=None, mysql_database=None, mysql_user=None, mysql_password=None,
         station_metadata_path=None):
    """
    Run all hypothesis analyses and optionally export to MySQL.
    
    Args:
        data_path: Path to final_dataset_long.csv
        output_dir: Directory for output files
        run_quality_checks: Whether to run data quality validation
        mysql_host: MySQL host (if None, skip MySQL export)
        mysql_database: MySQL database name
        mysql_user: MySQL username
        mysql_password: MySQL password
        station_metadata_path: Path to station metadata CSV with STATE column (for H5)
    """
    
    print("\n" + "="*80)
    print("CLIMATE PATTERN ANALYSIS - CS179G")
    print("="*80)
    
    # Initialize Spark
    spark = create_spark_session()
    print(f"\nSpark version: {spark.version}")
    print(f"Data path: {data_path}")
    
    try:
        # Load data
        print("\nLoading climate data...")
        df = load_climate_data(spark, data_path)
        
        record_count = df.count()
        print(f"Loaded {record_count:,} records")
        
        # Basic stats
        year_range = df.agg(min("year"), max("year")).collect()[0]
        station_count = df.select("STATION").distinct().count()
        print(f"Date range: {year_range[0]} - {year_range[1]}")
        print(f"Stations: {station_count:,}")
        
        # Element coverage
        print("\nElement coverage:")
        df.groupBy("ELEMENT").count().orderBy("count", ascending=False).show()
        
        # Optional quality checks
        if run_quality_checks:
            print("\nRunning data quality checks...")
            coverage = check_data_coverage(df)
            issues = validate_data_quality(df)
        
        # Run hypothesis analyses
        print("\n" + "="*80)
        print("RUNNING HYPOTHESIS TESTS")
        print("="*80)
        
        # Hypothesis 1: Winter Precipitation
        h1_df, h1_results = analyze_winter_precipitation_lag(df)
        
        # Hypothesis 2: Spring-Summer Heat
        h2_df, h2_results = analyze_spring_summer_heat(df)
        
        # Hypothesis 3: Wind Moderation
        h3_df, h3_results = analyze_wind_temperature_moderation(df)
        
        # Hypothesis 4: Snowmelt-Precipitation
        h4_df, h4_results = analyze_snowmelt_spring_precipitation(df)
        
        # Hypothesis 5: Regional Consistency
        # Note: Requires station metadata with STATE column
        h5_results = None
        if station_metadata_path:
            print("\nLoading station metadata for regional analysis...")
            stations_df = spark.read.csv(station_metadata_path, header=True, inferSchema=True)
            h5_results = analyze_regional_consistency(df, stations_df)
        else:
            print("\n" + "-"*60)
            print("HYPOTHESIS 5: Regional Consistency - SKIPPED")
            print("-"*60)
            print("To run regional analysis, provide station metadata:")
            print("  --stations path/to/selected_stations.csv")
        
        # Print summary
        print_summary(h1_results, h2_results, h3_results, h4_results)
        
        # Save results to JSON
        import os
        os.makedirs(output_dir, exist_ok=True)
        save_results_to_json(
            h1_results, h2_results, h3_results, h4_results,
            f"{output_dir}/analysis_results.json"
        )
        
        # =====================================================================
        # MYSQL EXPORT
        # =====================================================================
        if mysql_host and mysql_database and mysql_user and mysql_password:
            print("\n" + "="*80)
            print("EXPORTING TO MYSQL DATABASE")
            print("="*80)
            
            try:
                exporter = MySQLExporter(
                    host=mysql_host,
                    database=mysql_database,
                    user=mysql_user,
                    password=mysql_password
                )
                
                # Create tables
                exporter.create_tables()
                
                # Export detailed results
                print("\nExporting detailed results...")
                
                if h1_df is not None:
                    exporter.export_hypothesis1_results(h1_df)
                    exporter.export_summary(h1_results, "H1")
                
                if h2_df is not None:
                    exporter.export_hypothesis2_results(h2_df)
                    exporter.export_summary(h2_results, "H2")
                
                if h3_df is not None:
                    exporter.export_hypothesis3_results(h3_df)
                    exporter.export_summary(h3_results, "H3")
                
                if h4_df is not None:
                    exporter.export_hypothesis4_results(h4_df)
                    exporter.export_summary(h4_results, "H4")
                
                # Export H5 regional results
                if h5_results is not None:
                    from hypothesis5 import export_regional_results_to_mysql
                    export_regional_results_to_mysql(h5_results, exporter)
                
                exporter.close()
                
                print("\n✓ MySQL export completed successfully!")
                
            except Exception as e:
                print(f"\n✗ MySQL export failed: {e}")
                print("  Results were still saved to JSON.")
        else:
            print("\n" + "="*80)
            print("MYSQL EXPORT SKIPPED (no credentials provided)")
            print("="*80)
            print("To export to MySQL, run with:")
            print("  --mysql-host localhost --mysql-database climate_analysis \\")
            print("  --mysql-user root --mysql-password YOUR_PASSWORD")
        
        # Print MySQL table creation SQL for reference
        print("\n" + "="*80)
        print("MySQL Table Creation SQL (for manual setup):")
        print("="*80)
        print(create_mysql_tables_sql())
        
        return {
            "h1": (h1_df, h1_results),
            "h2": (h2_df, h2_results),
            "h3": (h3_df, h3_results),
            "h4": (h4_df, h4_results)
        }
        
    except Exception as e:
        print(f"\nError: {e}")
        print("\nMake sure the data file exists and has the correct format:")
        print("  STATION,DATE,ELEMENT,VALUE")
        print("  USC00045721,20200101,TMAX,15.5")
        raise
        
    finally:
        spark.stop()
        print("\nSpark session stopped.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Climate Pattern Analysis with Spark")
    parser.add_argument(
        "--data", 
        type=str, 
        default="data/final_dataset_long.csv",
        help="Path to the cleaned climate data CSV"
    )
    parser.add_argument(
        "--output", 
        type=str, 
        default="results",
        help="Output directory for results"
    )
    parser.add_argument(
        "--skip-quality-checks",
        action="store_true",
        help="Skip data quality validation"
    )
    
    # MySQL arguments
    parser.add_argument(
        "--mysql-host",
        type=str,
        default=None,
        help="MySQL host (e.g., localhost)"
    )
    parser.add_argument(
        "--mysql-port",
        type=int,
        default=3306,
        help="MySQL port (default: 3306)"
    )
    parser.add_argument(
        "--mysql-database",
        type=str,
        default="climate_analysis",
        help="MySQL database name"
    )
    parser.add_argument(
        "--mysql-user",
        type=str,
        default=None,
        help="MySQL username"
    )
    parser.add_argument(
        "--mysql-password",
        type=str,
        default=None,
        help="MySQL password"
    )
    parser.add_argument(
        "--stations",
        type=str,
        default=None,
        help="Path to station metadata CSV (required for H5 regional analysis)"
    )
    
    args = parser.parse_args()
    
    main(
        data_path=args.data,
        output_dir=args.output,
        run_quality_checks=not args.skip_quality_checks,
        mysql_host=args.mysql_host,
        mysql_database=args.mysql_database,
        mysql_user=args.mysql_user,
        mysql_password=args.mysql_password,
        station_metadata_path=args.stations
    )