import mysql.connector
from mysql.connector import Error


# =============================================================================
# TABLE SCHEMAS
# =============================================================================

CREATE_TABLES_SQL = """
-- ============================================================
-- Climate Pattern Analysis - MySQL Database Schema
-- ============================================================

-- Drop existing tables (optional - comment out if you want to keep data)
-- DROP TABLE IF EXISTS h1_winter_precipitation;
-- DROP TABLE IF EXISTS h2_spring_summer_heat;
-- DROP TABLE IF EXISTS h3_wind_moderation;
-- DROP TABLE IF EXISTS h4_snowmelt_precipitation;
-- DROP TABLE IF EXISTS analysis_summary;
-- DROP TABLE IF EXISTS regional_results;
-- DROP TABLE IF EXISTS stations;

-- Hypothesis 1: Winter Precipitation Lag
CREATE TABLE IF NOT EXISTS h1_winter_precipitation (
    id INT AUTO_INCREMENT PRIMARY KEY,
    station_id VARCHAR(20) NOT NULL,
    year INT NOT NULL,
    early_winter_prcp FLOAT,
    late_winter_prcp FLOAT,
    baseline_early FLOAT,
    baseline_late FLOAT,
    early_anomaly FLOAT,
    late_anomaly FLOAT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY unique_station_year (station_id, year),
    INDEX idx_year (year),
    INDEX idx_early_anomaly (early_anomaly),
    INDEX idx_late_anomaly (late_anomaly)
);

-- Hypothesis 2: Spring-Summer Heat
CREATE TABLE IF NOT EXISTS h2_spring_summer_heat (
    id INT AUTO_INCREMENT PRIMARY KEY,
    station_id VARCHAR(20) NOT NULL,
    year INT NOT NULL,
    spring_avg_tmax FLOAT,
    spring_temp_anomaly FLOAT,
    heat_wave_days INT DEFAULT 0,
    max_heat_wave_temp FLOAT,
    avg_heat_wave_temp FLOAT,
    extreme_days INT DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY unique_station_year (station_id, year),
    INDEX idx_year (year),
    INDEX idx_heat_wave_days (heat_wave_days),
    INDEX idx_spring_anomaly (spring_temp_anomaly)
);

-- Hypothesis 3: Wind Temperature Moderation
CREATE TABLE IF NOT EXISTS h3_wind_moderation (
    id INT AUTO_INCREMENT PRIMARY KEY,
    station_id VARCHAR(20) NOT NULL,
    event_date INT NOT NULL,
    awnd FLOAT,
    temp_range FLOAT,
    avg_range_before FLOAT,
    avg_range_after FLOAT,
    range_change FLOAT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY unique_station_date (station_id, event_date),
    INDEX idx_range_change (range_change),
    INDEX idx_event_date (event_date)
);

-- Hypothesis 4: Snowmelt-Precipitation
CREATE TABLE IF NOT EXISTS h4_snowmelt_precipitation (
    id INT AUTO_INCREMENT PRIMARY KEY,
    station_id VARCHAR(20) NOT NULL,
    year INT NOT NULL,
    snowmelt_doy INT,
    baseline_melt_doy FLOAT,
    melt_anomaly_days FLOAT,
    melt_anomaly_std FLOAT,
    spring_prcp FLOAT,
    baseline_spring_prcp FLOAT,
    prcp_anomaly_std FLOAT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY unique_station_year (station_id, year),
    INDEX idx_year (year),
    INDEX idx_melt_anomaly (melt_anomaly_days),
    INDEX idx_prcp_anomaly (prcp_anomaly_std)
);

-- Summary Statistics (aggregated results per hypothesis)
CREATE TABLE IF NOT EXISTS analysis_summary (
    id INT AUTO_INCREMENT PRIMARY KEY,
    hypothesis VARCHAR(20) NOT NULL,
    metric_name VARCHAR(50) NOT NULL,
    metric_value FLOAT,
    sample_size INT,
    description VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY unique_hypothesis_metric (hypothesis, metric_name)
);

-- Regional Comparison Results
CREATE TABLE IF NOT EXISTS regional_results (
    id INT AUTO_INCREMENT PRIMARY KEY,
    hypothesis VARCHAR(20) NOT NULL,
    region VARCHAR(30) NOT NULL,
    correlation FLOAT,
    sample_size INT,
    effect_size FLOAT,
    significant BOOLEAN,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY unique_hypothesis_region (hypothesis, region)
);

-- Station Metadata
CREATE TABLE IF NOT EXISTS stations (
    station_id VARCHAR(20) PRIMARY KEY,
    latitude FLOAT,
    longitude FLOAT,
    elevation FLOAT,
    state VARCHAR(2),
    name VARCHAR(100),
    region VARCHAR(30),
    INDEX idx_state (state),
    INDEX idx_region (region)
);

-- Yearly Aggregates (for trend visualization)
CREATE TABLE IF NOT EXISTS yearly_trends (
    id INT AUTO_INCREMENT PRIMARY KEY,
    year INT NOT NULL,
    metric_name VARCHAR(50) NOT NULL,
    avg_value FLOAT,
    min_value FLOAT,
    max_value FLOAT,
    std_value FLOAT,
    num_stations INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY unique_year_metric (year, metric_name),
    INDEX idx_year (year)
);
"""


# =============================================================================
# MYSQL EXPORTER CLASS
# =============================================================================

class MySQLExporter:
    """
    Export Spark analysis results to MySQL database.
    
    Usage:
        exporter = MySQLExporter(
            host="localhost",
            database="climate_analysis",
            user="root",
            password="your_password"
        )
        exporter.create_tables()
        exporter.export_hypothesis1_results(h1_dataframe)
        exporter.export_summary(h1_results, "H1")
        exporter.close()
    """
    
    def __init__(self, host, database, user, password, port=3306):
        """Initialize MySQL connection."""
        self.config = {
            "host": host,
            "database": database,
            "user": user,
            "password": password,
            "port": port
        }
        self.connection = None
        self.connect()
    
    def connect(self):
        """Establish database connection."""
        try:
            self.connection = mysql.connector.connect(**self.config)
            if self.connection.is_connected():
                print(f"Connected to MySQL database: {self.config['database']}")
        except Error as e:
            print(f"Error connecting to MySQL: {e}")
            raise
    
    def close(self):
        """Close database connection."""
        if self.connection and self.connection.is_connected():
            self.connection.close()
            print("MySQL connection closed.")
    
    def execute_sql(self, sql, params=None):
        """Execute a SQL statement."""
        cursor = self.connection.cursor()
        try:
            cursor.execute(sql, params)
            self.connection.commit()
        except Error as e:
            print(f"SQL Error: {e}")
            raise
        finally:
            cursor.close()
    
    def execute_many(self, sql, data):
        """Execute SQL with multiple rows of data."""
        cursor = self.connection.cursor()
        try:
            cursor.executemany(sql, data)
            self.connection.commit()
            return cursor.rowcount
        except Error as e:
            print(f"SQL Error: {e}")
            raise
        finally:
            cursor.close()
    
    # -------------------------------------------------------------------------
    # Table Creation
    # -------------------------------------------------------------------------
    
    def create_tables(self):
        """Create all required tables."""
        print("Creating database tables...")
        
        cursor = self.connection.cursor()
        
        # Split by statement and execute each
        statements = CREATE_TABLES_SQL.split(';')
        for statement in statements:
            statement = statement.strip()
            if statement and not statement.startswith('--'):
                try:
                    cursor.execute(statement)
                except Error as e:
                    # Ignore "table already exists" errors
                    if "already exists" not in str(e).lower():
                        print(f"Warning: {e}")
        
        self.connection.commit()
        cursor.close()
        print("Tables created successfully.")
    
    def drop_tables(self):
        """Drop all tables (use with caution!)."""
        tables = [
            "h1_winter_precipitation",
            "h2_spring_summer_heat", 
            "h3_wind_moderation",
            "h4_snowmelt_precipitation",
            "analysis_summary",
            "regional_results",
            "stations",
            "yearly_trends"
        ]
        
        cursor = self.connection.cursor()
        for table in tables:
            try:
                cursor.execute(f"DROP TABLE IF EXISTS {table}")
                print(f"Dropped table: {table}")
            except Error as e:
                print(f"Error dropping {table}: {e}")
        
        self.connection.commit()
        cursor.close()
    
    # -------------------------------------------------------------------------
    # Hypothesis 1: Winter Precipitation
    # -------------------------------------------------------------------------
    
    def export_hypothesis1_results(self, spark_df, batch_size=1000):
        """
        Export Hypothesis 1 results to MySQL.
        
        Args:
            spark_df: Spark DataFrame with columns:
                [STATION, year, early_winter_prcp, late_winter_prcp, 
                 baseline_early, baseline_late, early_anomaly, late_anomaly]
            batch_size: Number of rows per batch insert
        """
        print("Exporting Hypothesis 1 results...")
        
        # Collect data from Spark (for smaller datasets)
        rows = spark_df.select(
            "STATION", "year", "early_winter_prcp", "late_winter_prcp",
            "baseline_early", "baseline_late", "early_anomaly", "late_anomaly"
        ).collect()
        
        sql = """
            INSERT INTO h1_winter_precipitation 
            (station_id, year, early_winter_prcp, late_winter_prcp,
             baseline_early, baseline_late, early_anomaly, late_anomaly)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                early_winter_prcp = VALUES(early_winter_prcp),
                late_winter_prcp = VALUES(late_winter_prcp),
                early_anomaly = VALUES(early_anomaly),
                late_anomaly = VALUES(late_anomaly)
        """
        
        data = [
            (row["STATION"], row["year"], row["early_winter_prcp"], 
             row["late_winter_prcp"], row["baseline_early"], row["baseline_late"],
             row["early_anomaly"], row["late_anomaly"])
            for row in rows
        ]
        
        # Batch insert
        total = 0
        for i in range(0, len(data), batch_size):
            batch = data[i:i + batch_size]
            count = self.execute_many(sql, batch)
            total += count
        
        print(f"  Exported {total:,} rows to h1_winter_precipitation")
    
    # -------------------------------------------------------------------------
    # Hypothesis 2: Spring-Summer Heat
    # -------------------------------------------------------------------------
    
    def export_hypothesis2_results(self, spark_df, batch_size=1000):
        """
        Export Hypothesis 2 results to MySQL.
        
        Args:
            spark_df: Spark DataFrame with columns:
                [STATION, year, spring_avg_tmax, spring_temp_anomaly,
                 heat_wave_days, max_heat_wave_temp, extreme_days]
        """
        print("Exporting Hypothesis 2 results...")
        
        rows = spark_df.select(
            "STATION", "year", "spring_avg_tmax", "spring_temp_anomaly",
            "heat_wave_days", "max_heat_wave_temp", "extreme_days"
        ).collect()
        
        sql = """
            INSERT INTO h2_spring_summer_heat
            (station_id, year, spring_avg_tmax, spring_temp_anomaly,
             heat_wave_days, max_heat_wave_temp, extreme_days)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                spring_avg_tmax = VALUES(spring_avg_tmax),
                spring_temp_anomaly = VALUES(spring_temp_anomaly),
                heat_wave_days = VALUES(heat_wave_days),
                max_heat_wave_temp = VALUES(max_heat_wave_temp),
                extreme_days = VALUES(extreme_days)
        """
        
        data = [
            (row["STATION"], row["year"], row["spring_avg_tmax"],
             row["spring_temp_anomaly"], row["heat_wave_days"],
             row["max_heat_wave_temp"], row["extreme_days"])
            for row in rows
        ]
        
        total = 0
        for i in range(0, len(data), batch_size):
            batch = data[i:i + batch_size]
            count = self.execute_many(sql, batch)
            total += count
        
        print(f"  Exported {total:,} rows to h2_spring_summer_heat")
    
    # -------------------------------------------------------------------------
    # Hypothesis 3: Wind Moderation
    # -------------------------------------------------------------------------
    
    def export_hypothesis3_results(self, spark_df, batch_size=1000):
        """
        Export Hypothesis 3 results to MySQL.
        
        Args:
            spark_df: Spark DataFrame with columns:
                [STATION, DATE, AWND, temp_range, avg_range_before, 
                 avg_range_after, range_change]
        """
        print("Exporting Hypothesis 3 results...")
        
        rows = spark_df.select(
            "STATION", "DATE", "AWND", "temp_range",
            "avg_range_before", "avg_range_after", "range_change"
        ).collect()
        
        sql = """
            INSERT INTO h3_wind_moderation
            (station_id, event_date, awnd, temp_range,
             avg_range_before, avg_range_after, range_change)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                awnd = VALUES(awnd),
                temp_range = VALUES(temp_range),
                avg_range_before = VALUES(avg_range_before),
                avg_range_after = VALUES(avg_range_after),
                range_change = VALUES(range_change)
        """
        
        data = [
            (row["STATION"], row["DATE"], row["AWND"], row["temp_range"],
             row["avg_range_before"], row["avg_range_after"], row["range_change"])
            for row in rows
        ]
        
        total = 0
        for i in range(0, len(data), batch_size):
            batch = data[i:i + batch_size]
            count = self.execute_many(sql, batch)
            total += count
        
        print(f"  Exported {total:,} rows to h3_wind_moderation")
    
    # -------------------------------------------------------------------------
    # Hypothesis 4: Snowmelt-Precipitation
    # -------------------------------------------------------------------------
    
    def export_hypothesis4_results(self, spark_df, batch_size=1000):
        """
        Export Hypothesis 4 results to MySQL.
        
        Args:
            spark_df: Spark DataFrame with columns:
                [STATION, year, snowmelt_doy, baseline_melt_doy, melt_anomaly_days,
                 melt_anomaly_std, spring_prcp, baseline_spring_prcp, prcp_anomaly_std]
        """
        print("Exporting Hypothesis 4 results...")
        
        rows = spark_df.select(
            "STATION", "year", "snowmelt_doy", "baseline_melt_doy",
            "melt_anomaly_days", "melt_anomaly_std", "spring_prcp",
            "baseline_spring_prcp", "prcp_anomaly_std"
        ).collect()
        
        sql = """
            INSERT INTO h4_snowmelt_precipitation
            (station_id, year, snowmelt_doy, baseline_melt_doy, melt_anomaly_days,
             melt_anomaly_std, spring_prcp, baseline_spring_prcp, prcp_anomaly_std)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                snowmelt_doy = VALUES(snowmelt_doy),
                melt_anomaly_days = VALUES(melt_anomaly_days),
                spring_prcp = VALUES(spring_prcp),
                prcp_anomaly_std = VALUES(prcp_anomaly_std)
        """
        
        data = [
            (row["STATION"], row["year"], row["snowmelt_doy"],
             row["baseline_melt_doy"], row["melt_anomaly_days"],
             row["melt_anomaly_std"], row["spring_prcp"],
             row["baseline_spring_prcp"], row["prcp_anomaly_std"])
            for row in rows
        ]
        
        total = 0
        for i in range(0, len(data), batch_size):
            batch = data[i:i + batch_size]
            count = self.execute_many(sql, batch)
            total += count
        
        print(f"  Exported {total:,} rows to h4_snowmelt_precipitation")
    
    # -------------------------------------------------------------------------
    # Summary Statistics
    # -------------------------------------------------------------------------
    
    def export_summary(self, results_dict, hypothesis_name):
        """
        Export summary statistics for a hypothesis.
        
        Args:
            results_dict: Dictionary with metric names and values
            hypothesis_name: e.g., "H1", "H2", "H3", "H4"
        """
        print(f"Exporting summary for {hypothesis_name}...")
        
        sql = """
            INSERT INTO analysis_summary
            (hypothesis, metric_name, metric_value, sample_size, description)
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                metric_value = VALUES(metric_value),
                sample_size = VALUES(sample_size)
        """
        
        data = []
        sample_size = results_dict.get("sample_size")
        
        for key, value in results_dict.items():
            if key == "sample_size":
                continue
            if isinstance(value, (int, float)) and value is not None:
                data.append((hypothesis_name, key, float(value), sample_size, None))
        
        if data:
            self.execute_many(sql, data)
            print(f"  Exported {len(data)} summary metrics for {hypothesis_name}")
    
    # -------------------------------------------------------------------------
    # Station Metadata
    # -------------------------------------------------------------------------
    
    def export_stations(self, stations_df, batch_size=500):
        """
        Export station metadata to MySQL.
        
        Args:
            stations_df: DataFrame with station metadata
        """
        print("Exporting station metadata...")
        
        rows = stations_df.collect()
        
        sql = """
            INSERT INTO stations
            (station_id, latitude, longitude, elevation, state, name, region)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                latitude = VALUES(latitude),
                longitude = VALUES(longitude),
                elevation = VALUES(elevation),
                state = VALUES(state),
                name = VALUES(name),
                region = VALUES(region)
        """
        
        data = [
            (row.get("STATION_ID") or row.get("station_id"),
             row.get("LATITUDE") or row.get("latitude"),
             row.get("LONGITUDE") or row.get("longitude"),
             row.get("ELEVATION") or row.get("elevation"),
             row.get("STATE") or row.get("state"),
             row.get("NAME") or row.get("name"),
             row.get("REGION") or row.get("region"))
            for row in rows
        ]
        
        total = 0
        for i in range(0, len(data), batch_size):
            batch = data[i:i + batch_size]
            count = self.execute_many(sql, batch)
            total += count
        
        print(f"  Exported {total:,} stations")
    
    # -------------------------------------------------------------------------
    # Yearly Trends
    # -------------------------------------------------------------------------
    
    def export_yearly_trends(self, trends_df, metric_name):
        """
        Export yearly trend data for visualization.
        
        Args:
            trends_df: DataFrame with year, avg, min, max, std, count
            metric_name: Name of the metric (e.g., "avg_tmax", "heat_wave_days")
        """
        print(f"Exporting yearly trends for {metric_name}...")
        
        rows = trends_df.collect()
        
        sql = """
            INSERT INTO yearly_trends
            (year, metric_name, avg_value, min_value, max_value, std_value, num_stations)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                avg_value = VALUES(avg_value),
                min_value = VALUES(min_value),
                max_value = VALUES(max_value),
                std_value = VALUES(std_value),
                num_stations = VALUES(num_stations)
        """
        
        data = [
            (row["year"], metric_name, 
             row.get("avg_value") or row.get("avg"),
             row.get("min_value") or row.get("min"),
             row.get("max_value") or row.get("max"),
             row.get("std_value") or row.get("std"),
             row.get("num_stations") or row.get("count"))
            for row in rows
        ]
        
        self.execute_many(sql, data)
        print(f"  Exported {len(data)} yearly trend records")


# =============================================================================
# SPARK JDBC EXPORT (for large DataFrames)
# =============================================================================

def get_jdbc_properties(user, password):
    """Get JDBC connection properties."""
    return {
        "user": user,
        "password": password,
        "driver": "com.mysql.cj.jdbc.Driver"
    }


def get_jdbc_url(host, port, database):
    """Get JDBC connection URL."""
    return f"jdbc:mysql://{host}:{port}/{database}"


def export_via_spark_jdbc(spark_df, table_name, jdbc_url, properties, mode="overwrite"):
    """
    Export large Spark DataFrame directly to MySQL via JDBC.
    
    Requires: mysql-connector-java-8.x.jar in Spark classpath
    
    Args:
        spark_df: Spark DataFrame
        table_name: Target MySQL table
        jdbc_url: JDBC URL (e.g., "jdbc:mysql://localhost:3306/climate_analysis")
        properties: Dict with "user", "password", "driver"
        mode: Write mode (overwrite, append, ignore, error)
    """
    print(f"Exporting to {table_name} via Spark JDBC...")
    
    spark_df.write \
        .mode(mode) \
        .jdbc(url=jdbc_url, table=table_name, properties=properties)
    
    print(f"  Exported {spark_df.count():,} rows to {table_name}")


# =============================================================================
# CONVENIENCE FUNCTION
# =============================================================================

def export_all_results(h1_df, h1_results, h2_df, h2_results, 
                       h3_df, h3_results, h4_df, h4_results,
                       host="localhost", database="climate_analysis",
                       user="root", password="password", port=3306):
    """
    Export all hypothesis results to MySQL in one call.
    
    Args:
        h1_df, h2_df, h3_df, h4_df: Spark DataFrames with detailed results
        h1_results, h2_results, h3_results, h4_results: Summary dicts
        host, database, user, password, port: MySQL connection params
    """
    exporter = MySQLExporter(host, database, user, password, port)
    
    try:
        # Create tables
        exporter.create_tables()
        
        # Export detailed results
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
        
        print("\n" + "="*60)
        print("All results exported successfully!")
        print("="*60)
        
    finally:
        exporter.close()


# =============================================================================
# COMMAND LINE INTERFACE
# =============================================================================

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="MySQL Export Utility")
    parser.add_argument("--host", default="localhost", help="MySQL host")
    parser.add_argument("--port", type=int, default=3306, help="MySQL port")
    parser.add_argument("--database", default="climate_analysis", help="Database name")
    parser.add_argument("--user", default="root", help="MySQL user")
    parser.add_argument("--password", required=True, help="MySQL password")
    parser.add_argument("--create-tables", action="store_true", help="Create tables only")
    parser.add_argument("--drop-tables", action="store_true", help="Drop all tables (DANGER!)")
    
    args = parser.parse_args()
    
    exporter = MySQLExporter(
        host=args.host,
        port=args.port,
        database=args.database,
        user=args.user,
        password=args.password
    )
    
    try:
        if args.drop_tables:
            confirm = input("Are you sure you want to DROP ALL TABLES? (yes/no): ")
            if confirm.lower() == "yes":
                exporter.drop_tables()
        elif args.create_tables:
            exporter.create_tables()
        else:
            print("Use --create-tables to create database schema")
            print("Use --drop-tables to remove all tables")
    finally:
        exporter.close()