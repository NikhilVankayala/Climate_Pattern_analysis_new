from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, sum, count, when, stddev, min, max, corr
)


# =============================================================================
# REGION MAPPING
# =============================================================================

REGION_MAPPING = {
    'Northeast': ['ME', 'NH', 'VT', 'MA', 'RI', 'CT', 'NY', 'NJ', 'PA'],
    'Southeast': ['DE', 'MD', 'VA', 'WV', 'NC', 'SC', 'GA', 'FL', 'AL', 'MS', 'LA', 'AR', 'TN', 'KY'],
    'Midwest': ['OH', 'IN', 'IL', 'MI', 'WI', 'MN', 'IA', 'MO', 'ND', 'SD', 'NE', 'KS'],
    'Southwest': ['TX', 'OK', 'NM', 'AZ'],
    'West': ['CA', 'NV', 'UT', 'CO', 'WY', 'MT', 'ID', 'OR', 'WA'],
    'Alaska': ['AK'],
    'Hawaii': ['HI']
}


def get_state_to_region_mapping():
    """Return state to region lookup dictionary"""
    state_to_region = {}
    for region, states in REGION_MAPPING.items():
        for state in states:
            state_to_region[state] = region
    return state_to_region


def add_region_column(df, station_metadata_df):
    """
    Add region column to climate data based on station state.
    
    Args:
        df: Climate data DataFrame with STATION column
        station_metadata_df: Station metadata with STATION_ID and STATE columns
        
    Returns:
        DataFrame with REGION column added
    """
    spark = SparkSession.builder.getOrCreate()
    
    state_to_region = get_state_to_region_mapping()
    region_data = [(state, region) for state, region in state_to_region.items()]
    region_df = spark.createDataFrame(region_data, ["STATE", "REGION"])
    
    # Join station metadata with region mapping
    stations_with_region = station_metadata_df.join(region_df, "STATE", "left")
    
    # Join climate data with station regions
    df_with_region = df.join(
        stations_with_region.select(
            col("STATION_ID").alias("STATION"), 
            "REGION"
        ),
        "STATION",
        "left"
    )
    
    return df_with_region


# =============================================================================
# DATA QUALITY CHECKS
# =============================================================================

def check_data_coverage(df):
    """
    Check data coverage by element and provide summary statistics.
    
    Args:
        df: Climate data DataFrame
        
    Returns:
        dict: Coverage statistics
    """
    print("\n" + "="*60)
    print("DATA COVERAGE REPORT")
    print("="*60)
    
    # Overall stats
    total_records = df.count()
    total_stations = df.select("STATION").distinct().count()
    
    print(f"\nTotal records: {total_records:,}")
    print(f"Total stations: {total_stations:,}")
    
    # Date range
    year_range = df.agg(min("year"), max("year")).collect()[0]
    print(f"Year range: {year_range[0]} - {year_range[1]}")
    
    # Coverage by element
    print("\nRecords by element:")
    element_counts = df.groupBy("ELEMENT").agg(
        count("*").alias("records")
    ).orderBy("records", ascending=False)
    element_counts.show()
    
    # Collect element coverage for return
    elements = element_counts.collect()
    coverage = {row["ELEMENT"]: row["records"] / total_records for row in elements}
    
    # Check key elements
    print("\nKey element coverage:")
    for element in ["TMAX", "TMIN", "PRCP", "AWND", "SNWD"]:
        elem_count = df.filter(col("ELEMENT") == element).count()
        pct = elem_count / total_records * 100 if total_records > 0 else 0
        elem_stations = df.filter(col("ELEMENT") == element).select("STATION").distinct().count()
        station_pct = elem_stations / total_stations * 100 if total_stations > 0 else 0
        print(f"  {element}: {elem_count:,} records ({pct:.1f}%), {elem_stations:,} stations ({station_pct:.1f}%)")
    
    return {
        "total_records": total_records,
        "total_stations": total_stations,
        "year_range": (year_range[0], year_range[1]),
        "element_coverage": coverage
    }


def validate_data_quality(df):
    """
    Check for data quality issues.
    
    Args:
        df: Climate data DataFrame
        
    Returns:
        list: Quality issues found
    """
    print("\n" + "="*60)
    print("DATA QUALITY VALIDATION")
    print("="*60)
    
    issues = []
    
    # Check for extreme temperature values
    extreme_high_temp = df.filter(
        ((col("ELEMENT") == "TMAX") & (col("VALUE") > 60)) |
        ((col("ELEMENT") == "TMIN") & (col("VALUE") > 50))
    ).count()
    
    extreme_low_temp = df.filter(
        ((col("ELEMENT") == "TMAX") & (col("VALUE") < -60)) |
        ((col("ELEMENT") == "TMIN") & (col("VALUE") < -70))
    ).count()
    
    if extreme_high_temp > 0:
        issues.append(f"Found {extreme_high_temp} extremely high temperature readings (>60°C)")
        
    if extreme_low_temp > 0:
        issues.append(f"Found {extreme_low_temp} extremely low temperature readings (<-70°C)")
    
    # Check for negative precipitation
    neg_prcp = df.filter(
        (col("ELEMENT") == "PRCP") & (col("VALUE") < 0)
    ).count()
    
    if neg_prcp > 0:
        issues.append(f"Found {neg_prcp} negative precipitation values")
    
    # Check for null values
    null_values = df.filter(col("VALUE").isNull()).count()
    if null_values > 0:
        issues.append(f"Found {null_values} null VALUE entries")
    
    # Check for duplicate records
    total = df.count()
    distinct = df.select("STATION", "DATE", "ELEMENT").distinct().count()
    duplicates = total - distinct
    
    if duplicates > 0:
        issues.append(f"Found {duplicates} duplicate station-date-element combinations")
    
    # Print results
    if issues:
        print("\nQuality issues found:")
        for issue in issues:
            print(f"  ⚠ {issue}")
    else:
        print("\n✓ No quality issues detected!")
    
    return issues


# =============================================================================
# STATISTICAL HELPERS
# =============================================================================

def calculate_correlation_with_significance(df, col1, col2):
    """
    Calculate Pearson correlation with approximate significance test.
    
    Args:
        df: DataFrame
        col1, col2: Column names to correlate
        
    Returns:
        dict: Correlation results with significance info
    """
    import math
    
    # Calculate correlation
    r = df.select(corr(col1, col2)).collect()[0][0]
    
    # Count valid pairs
    n = df.filter(col(col1).isNotNull() & col(col2).isNotNull()).count()
    
    if r is None or n < 3:
        return {"r": None, "n": n, "significant": False, "p_approx": None}
    
    # Calculate t-statistic
    if abs(r) < 1:
        t_stat = r * math.sqrt((n - 2) / (1 - r**2))
    else:
        t_stat = float('inf')
    
    # Approximate p-value check (two-tailed)
    # |t| > 1.96 corresponds to p < 0.05 for large n
    # |t| > 2.576 corresponds to p < 0.01
    significant_05 = abs(t_stat) > 1.96
    significant_01 = abs(t_stat) > 2.576
    
    return {
        "r": r,
        "n": n,
        "t_stat": t_stat,
        "significant_05": significant_05,
        "significant_01": significant_01
    }


def calculate_effect_size(group1_mean, group2_mean, pooled_std):
    """
    Calculate Cohen's d effect size.
    
    Args:
        group1_mean: Mean of group 1
        group2_mean: Mean of group 2
        pooled_std: Pooled standard deviation
        
    Returns:
        float: Cohen's d value
    """
    if pooled_std is None or pooled_std == 0:
        return None
    return (group1_mean - group2_mean) / pooled_std


# =============================================================================
# MYSQL EXPORT HELPERS
# =============================================================================

def create_mysql_tables_sql():
    """Generate SQL for creating results tables in MySQL."""
    
    sql = """
-- ============================================================
-- Climate Pattern Analysis - MySQL Table Schema
-- ============================================================

-- Hypothesis 1: Winter Precipitation Lag
CREATE TABLE IF NOT EXISTS h1_winter_precipitation (
    id INT AUTO_INCREMENT PRIMARY KEY,
    station_id VARCHAR(20) NOT NULL,
    year INT NOT NULL,
    early_winter_prcp DECIMAL(10,2),
    late_winter_prcp DECIMAL(10,2),
    early_anomaly DECIMAL(10,4),
    late_anomaly DECIMAL(10,4),
    UNIQUE KEY unique_station_year (station_id, year),
    INDEX idx_year (year),
    INDEX idx_early_anomaly (early_anomaly)
);

-- Hypothesis 2: Spring-Summer Heat
CREATE TABLE IF NOT EXISTS h2_spring_summer_heat (
    id INT AUTO_INCREMENT PRIMARY KEY,
    station_id VARCHAR(20) NOT NULL,
    year INT NOT NULL,
    spring_avg_tmax DECIMAL(10,2),
    spring_temp_anomaly DECIMAL(10,4),
    heat_wave_days INT DEFAULT 0,
    max_heat_wave_temp DECIMAL(10,2),
    extreme_days INT DEFAULT 0,
    UNIQUE KEY unique_station_year (station_id, year),
    INDEX idx_year (year),
    INDEX idx_heat_wave_days (heat_wave_days)
);

-- Hypothesis 3: Wind Temperature Moderation
CREATE TABLE IF NOT EXISTS h3_wind_moderation (
    id INT AUTO_INCREMENT PRIMARY KEY,
    station_id VARCHAR(20) NOT NULL,
    event_date INT NOT NULL,
    awnd DECIMAL(10,2),
    temp_range_before DECIMAL(10,2),
    temp_range_after DECIMAL(10,2),
    range_change DECIMAL(10,2),
    UNIQUE KEY unique_station_date (station_id, event_date),
    INDEX idx_range_change (range_change)
);

-- Hypothesis 4: Snowmelt-Precipitation
CREATE TABLE IF NOT EXISTS h4_snowmelt_precipitation (
    id INT AUTO_INCREMENT PRIMARY KEY,
    station_id VARCHAR(20) NOT NULL,
    year INT NOT NULL,
    snowmelt_doy INT,
    melt_anomaly_days DECIMAL(10,2),
    spring_prcp DECIMAL(10,2),
    prcp_anomaly_std DECIMAL(10,4),
    UNIQUE KEY unique_station_year (station_id, year),
    INDEX idx_year (year),
    INDEX idx_melt_anomaly (melt_anomaly_days)
);

-- Summary Statistics (one row per hypothesis-metric)
CREATE TABLE IF NOT EXISTS analysis_summary (
    id INT AUTO_INCREMENT PRIMARY KEY,
    hypothesis VARCHAR(10) NOT NULL,
    metric VARCHAR(50) NOT NULL,
    value DECIMAL(15,6),
    sample_size INT,
    is_significant BOOLEAN,
    analysis_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY unique_hypothesis_metric (hypothesis, metric)
);

-- Regional Comparison Results
CREATE TABLE IF NOT EXISTS regional_results (
    id INT AUTO_INCREMENT PRIMARY KEY,
    hypothesis VARCHAR(10) NOT NULL,
    region VARCHAR(20) NOT NULL,
    correlation DECIMAL(10,6),
    effect_size DECIMAL(10,6),
    sample_size INT,
    UNIQUE KEY unique_hypothesis_region (hypothesis, region)
);

-- Station Metadata (for joins)
CREATE TABLE IF NOT EXISTS stations (
    station_id VARCHAR(20) PRIMARY KEY,
    latitude DECIMAL(10,6),
    longitude DECIMAL(10,6),
    elevation DECIMAL(10,2),
    state VARCHAR(2),
    name VARCHAR(100),
    region VARCHAR(20),
    INDEX idx_state (state),
    INDEX idx_region (region)
);
"""
    return sql


def get_mysql_connection_properties(host, port, database, user, password):
    """
    Get JDBC connection properties for MySQL.
    
    Args:
        host: MySQL host
        port: MySQL port
        database: Database name
        user: Username
        password: Password
        
    Returns:
        tuple: (jdbc_url, properties_dict)
    """
    jdbc_url = f"jdbc:mysql://{host}:{port}/{database}"
    properties = {
        "user": user,
        "password": password,
        "driver": "com.mysql.cj.jdbc.Driver"
    }
    return jdbc_url, properties


def export_to_mysql(df, table_name, jdbc_url, properties, mode="overwrite"):
    """
    Export Spark DataFrame to MySQL table.
    
    Args:
        df: Spark DataFrame to export
        table_name: Target table name
        jdbc_url: JDBC connection URL
        properties: Connection properties dict
        mode: Write mode (overwrite, append, ignore, error)
    """
    df.write \
        .mode(mode) \
        .jdbc(url=jdbc_url, table=table_name, properties=properties)
    
    print(f"Exported {df.count():,} rows to {table_name}")


# =============================================================================
# VISUALIZATION DATA PREP
# =============================================================================

def prepare_timeseries_for_viz(df, group_cols, value_col, time_col="year"):
    """
    Prepare time series data for visualization export.
    
    Args:
        df: Source DataFrame
        group_cols: Columns to group by (besides time)
        value_col: Column to aggregate
        time_col: Time column name
        
    Returns:
        DataFrame ready for visualization
    """
    return df.groupBy(*group_cols, time_col) \
        .agg(
            avg(value_col).alias(f"avg_{value_col}"),
            stddev(value_col).alias(f"std_{value_col}"),
            min(value_col).alias(f"min_{value_col}"),
            max(value_col).alias(f"max_{value_col}"),
            count("*").alias("n")
        ) \
        .orderBy(time_col)


def export_for_visualization(df, output_path, format="csv"):
    """
    Export DataFrame to CSV or JSON for web visualization.
    
    Args:
        df: DataFrame to export
        output_path: Output file/directory path
        format: Output format (csv or json)
    """
    if format == "csv":
        df.coalesce(1) \
            .write \
            .mode("overwrite") \
            .option("header", "true") \
            .csv(output_path)
    elif format == "json":
        df.coalesce(1) \
            .write \
            .mode("overwrite") \
            .json(output_path)
    else:
        raise ValueError(f"Unsupported format: {format}")
    
    print(f"Exported to {output_path} ({format})")


# =============================================================================
# SAMPLE DATA GENERATOR (for testing)
# =============================================================================

def generate_sample_data(spark, n_stations=10, n_years=3):
    """
    Generate sample climate data for testing.
    
    Args:
        spark: SparkSession
        n_stations: Number of stations
        n_years: Number of years
        
    Returns:
        DataFrame with sample data
    """
    import random
    from datetime import date
    
    data = []
    elements = ['TMAX', 'TMIN', 'PRCP', 'SNOW', 'SNWD', 'AWND']
    
    for station_idx in range(n_stations):
        station_id = f"USC{station_idx:08d}"
        
        for year in range(2020, 2020 + n_years):
            for day_of_year in range(1, 366):
                # Calculate date integer (YYYYMMDD)
                try:
                    d = date(year, 1, 1)
                    d = date.fromordinal(d.toordinal() + day_of_year - 1)
                    date_int = int(d.strftime('%Y%m%d'))
                except:
                    continue
                
                # Generate values for each element
                for element in elements:
                    # Seasonal temperature pattern
                    seasonal = 15 * (1 - abs(day_of_year - 182) / 182)
                    
                    if element == 'TMAX':
                        value = 10 + seasonal + random.gauss(0, 5)
                    elif element == 'TMIN':
                        value = seasonal + random.gauss(0, 5)
                    elif element == 'PRCP':
                        value = max(0, random.expovariate(1/3))
                    elif element == 'SNOW':
                        if day_of_year < 90 or day_of_year > 320:
                            value = max(0, random.gauss(20, 15))
                        else:
                            value = 0
                    elif element == 'SNWD':
                        if day_of_year < 100 or day_of_year > 310:
                            value = max(0, random.gauss(50, 30))
                        else:
                            value = 0
                    elif element == 'AWND':
                        value = max(0, random.gauss(3.5, 1.5))
                    
                    data.append((station_id, date_int, element, round(value, 1)))
    
    schema = ["STATION", "DATE", "ELEMENT", "VALUE"]
    df = spark.createDataFrame(data, schema)
    
    # Add derived columns
    df = df.withColumn("year", (col("DATE") / 10000).cast("int")) \
           .withColumn("month", ((col("DATE") % 10000) / 100).cast("int")) \
           .withColumn("day", (col("DATE") % 100).cast("int"))
    
    return df


# =============================================================================
# RESULTS FORMATTING
# =============================================================================

def format_results_summary(results_dict, hypothesis_name):
    """Format results dictionary as readable summary string."""
    
    lines = [
        "",
        "=" * 60,
        hypothesis_name,
        "=" * 60
    ]
    
    for key, value in results_dict.items():
        if value is None:
            formatted = "N/A"
        elif isinstance(value, float):
            formatted = f"{value:.4f}"
        else:
            formatted = str(value)
        lines.append(f"  {key}: {formatted}")
    
    return "\n".join(lines)


def generate_latex_table(results_list, caption, label):
    """
    Generate LaTeX table from results list.
    
    Args:
        results_list: List of result dicts with keys: name, correlation, n, significant
        caption: Table caption
        label: LaTeX label
        
    Returns:
        str: LaTeX table code
    """
    latex = f"""
\\begin{{table}}[h]
\\centering
\\caption{{{caption}}}
\\label{{{label}}}
\\begin{{tabular}}{{lccc}}
\\toprule
\\textbf{{Hypothesis}} & \\textbf{{Correlation}} & \\textbf{{N}} & \\textbf{{Significant}} \\\\
\\midrule
"""
    
    for result in results_list:
        corr_val = f"{result['correlation']:.4f}" if result.get('correlation') else "N/A"
        n_val = f"{result.get('n', 'N/A'):,}" if isinstance(result.get('n'), int) else "N/A"
        sig_val = "Yes" if result.get('significant') else "No"
        
        latex += f"{result['name']} & {corr_val} & {n_val} & {sig_val} \\\\\n"
    
    latex += """\\bottomrule
\\end{tabular}
\\end{table}
"""
    return latex


# =============================================================================
# TEST / DEMO
# =============================================================================

if __name__ == "__main__":
    print("Climate Analysis Utilities")
    print("=" * 40)
    
    # Create test Spark session
    spark = SparkSession.builder \
        .appName("UtilityTest") \
        .master("local[*]") \
        .getOrCreate()
    
    print("\nGenerating sample data...")
    sample_df = generate_sample_data(spark, n_stations=5, n_years=2)
    print(f"Generated {sample_df.count():,} records")
    
    print("\nSample data preview:")
    sample_df.show(10)
    
    print("\nRunning data coverage check...")
    coverage = check_data_coverage(sample_df)
    
    print("\nRunning data quality validation...")
    issues = validate_data_quality(sample_df)
    
    print("\nMySQL table creation SQL preview:")
    sql = create_mysql_tables_sql()
    print(sql[:500] + "...")
    
    spark.stop()
    print("\nDone!")