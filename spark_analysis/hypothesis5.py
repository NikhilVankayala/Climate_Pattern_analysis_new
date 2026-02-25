from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit

# Import hypothesis analysis functions
from hypothesis1 import analyze_winter_precipitation_lag
from hypothesis2 import analyze_spring_summer_heat
from hypothesis3 import analyze_wind_temperature_moderation
from hypothesis4 import analyze_snowmelt_spring_precipitation

# REGION DEFINITIONS

REGION_MAPPING = {
    'Northeast': ['ME', 'NH', 'VT', 'MA', 'RI', 'CT', 'NY', 'NJ', 'PA'],
    'Southeast': ['DE', 'MD', 'VA', 'WV', 'NC', 'SC', 'GA', 'FL', 'AL', 'MS', 'LA', 'AR', 'TN', 'KY'],
    'Midwest': ['OH', 'IN', 'IL', 'MI', 'WI', 'MN', 'IA', 'MO', 'ND', 'SD', 'NE', 'KS'],
    'Southwest': ['TX', 'OK', 'NM', 'AZ'],
    'West': ['CA', 'NV', 'UT', 'CO', 'WY', 'MT', 'ID', 'OR', 'WA'],
    'Alaska': ['AK'],
    'Hawaii': ['HI']
}


def get_state_to_region():
    """Return state to region lookup dictionary."""
    state_to_region = {}
    for region, states in REGION_MAPPING.items():
        for state in states:
            state_to_region[state] = region
    return state_to_region


# ADD REGION TO DATA

def add_region_column(df, station_metadata_df):
    """
    Add region column to climate data based on station state.
    
    Args:
        df: Climate data DataFrame with STATION column
        station_metadata_df: Station metadata DataFrame with STATION_ID and STATE columns
        
    Returns:
        DataFrame with REGION column added
    """
    spark = SparkSession.builder.getOrCreate()
    
    # Create state-to-region mapping DataFrame
    state_to_region = get_state_to_region()
    region_data = [(state, region) for state, region in state_to_region.items()]
    region_df = spark.createDataFrame(region_data, ["STATE", "REGION"])
    
    # Join station metadata with region mapping
    stations_with_region = station_metadata_df.join(region_df, "STATE", "left")
    
    # Join climate data with station regions
    # Handle different possible column names for station ID
    station_id_col = "STATION_ID" if "STATION_ID" in station_metadata_df.columns else "station_id"
    
    df_with_region = df.join(
        stations_with_region.select(
            col(station_id_col).alias("_station_id"),
            "REGION"
        ),
        df["STATION"] == col("_station_id"),
        "left"
    ).drop("_station_id")
    
    return df_with_region


def add_region_from_station_id(df):
    """
    Add region column by extracting state from station ID.
    
    NOAA station IDs often have format like "USC00045721" where characters
    don't directly encode state, but if your station IDs include state codes,
    this can be adapted.
    
    For GHCN data, you need the station metadata file to get state info.
    This is a fallback that assigns "Unknown" if no metadata is available.
    
    Args:
        df: Climate data DataFrame
        
    Returns:
        DataFrame with REGION column (may be "Unknown" without metadata)
    """
    return df.withColumn("REGION", lit("Unknown"))

# REGIONAL ANALYSIS

def analyze_regional_consistency(df, station_metadata_df=None, verbose=True):
    """
    Run all hypothesis analyses by region and compare results.
    
    Args:
        df: Climate data DataFrame with [STATION, DATE, ELEMENT, VALUE, year, month]
        station_metadata_df: Station metadata with STATE column (optional)
        verbose: Print detailed output
        
    Returns:
        dict: Regional results for all hypotheses
    """
    
    if verbose:
        print("\n" + "="*80)
        print("HYPOTHESIS 5: Regional Consistency Analysis")
        print("="*80)
    
    # -------------------------------------------------------------------------
    # Step 1: Add region column to data
    # -------------------------------------------------------------------------
    if station_metadata_df is not None:
        if verbose:
            print("\nAdding region based on station metadata...")
        df_with_region = add_region_column(df, station_metadata_df)
    else:
        if verbose:
            print("\nWARNING: No station metadata provided.")
            print("  To run regional analysis, provide station metadata with STATE column.")
            print("  Attempting to continue without regional breakdown...")
        return None
    
    # Check region coverage
    region_counts = df_with_region.groupBy("REGION").count().collect()
    
    if verbose:
        print("\nRecords by region:")
        for row in sorted(region_counts, key=lambda x: x["count"], reverse=True):
            print(f"  {row['REGION']}: {row['count']:,}")
    
    # Step 2: Run each hypothesis by region
    regions = [row["REGION"] for row in region_counts if row["REGION"] is not None]
    
    all_results = {
        "H1_winter_precipitation": {},
        "H2_spring_summer_heat": {},
        "H3_wind_moderation": {},
        "H4_snowmelt_precipitation": {}
    }
    
    for region in regions:
        if verbose:
            print(f"\n{'='*60}")
            print(f"ANALYZING REGION: {region}")
            print(f"{'='*60}")
        
        # Filter data for this region
        region_df = df_with_region.filter(col("REGION") == region)
        record_count = region_df.count()
        
        if record_count < 10000:
            if verbose:
                print(f"  Skipping {region}: insufficient data ({record_count:,} records)")
            continue
        
        # ----- H1: Winter Precipitation -----
        try:
            _, h1_results = analyze_winter_precipitation_lag(region_df, verbose=False)
            all_results["H1_winter_precipitation"][region] = {
                "correlation": h1_results.get("correlation"),
                "sample_size": h1_results.get("sample_size"),
                "compensation_rate": h1_results.get("compensation_rate")
            }
            if verbose:
                corr = h1_results.get("correlation")
                print(f"  H1 Winter Precip - Correlation: {corr:.4f}" if corr else "  H1: No data")
        except Exception as e:
            if verbose:
                print(f"  H1 Error: {e}")
            all_results["H1_winter_precipitation"][region] = {"error": str(e)}
        
        # ----- H2: Spring-Summer Heat -----
        try:
            _, h2_results = analyze_spring_summer_heat(region_df, verbose=False)
            all_results["H2_spring_summer_heat"][region] = {
                "correlation": h2_results.get("correlation_days"),
                "sample_size": h2_results.get("sample_size"),
                "avg_hw_warm": h2_results.get("avg_hw_warm_spring"),
                "avg_hw_cool": h2_results.get("avg_hw_cool_spring")
            }
            if verbose:
                corr = h2_results.get("correlation_days")
                print(f"  H2 Spring-Summer Heat - Correlation: {corr:.4f}" if corr else "  H2: No data")
        except Exception as e:
            if verbose:
                print(f"  H2 Error: {e}")
            all_results["H2_spring_summer_heat"][region] = {"error": str(e)}
        
        # ----- H3: Wind Moderation -----
        try:
            _, h3_results = analyze_wind_temperature_moderation(region_df, verbose=False)
            if h3_results.get("error"):
                all_results["H3_wind_moderation"][region] = {"error": h3_results["error"]}
            else:
                all_results["H3_wind_moderation"][region] = {
                    "avg_range_change": h3_results.get("avg_range_change"),
                    "moderation_rate": h3_results.get("moderation_rate"),
                    "num_events": h3_results.get("num_events")
                }
            if verbose:
                rate = h3_results.get("moderation_rate")
                print(f"  H3 Wind Moderation - Rate: {rate:.1f}%" if rate else "  H3: Limited wind data")
        except Exception as e:
            if verbose:
                print(f"  H3 Error: {e}")
            all_results["H3_wind_moderation"][region] = {"error": str(e)}
        
        # ----- H4: Snowmelt-Precipitation -----
        try:
            _, h4_results = analyze_snowmelt_spring_precipitation(region_df, verbose=False)
            if h4_results.get("error"):
                all_results["H4_snowmelt_precipitation"][region] = {"error": h4_results["error"]}
            else:
                all_results["H4_snowmelt_precipitation"][region] = {
                    "correlation": h4_results.get("correlation"),
                    "sample_size": h4_results.get("sample_size"),
                    "early_melt_prcp": h4_results.get("early_melt_prcp_anomaly")
                }
            if verbose:
                corr = h4_results.get("correlation")
                if corr:
                    print(f"  H4 Snowmelt-Precip - Correlation: {corr:.4f}")
                else:
                    print(f"  H4: No snow data for this region")
        except Exception as e:
            if verbose:
                print(f"  H4 Error: {e}")
            all_results["H4_snowmelt_precipitation"][region] = {"error": str(e)}
    
    # Step 3: Compile summary comparison
    if verbose:
        print_regional_summary(all_results)
    
    return all_results


# SUMMARY OUTPUT

def print_regional_summary(all_results):
    """Print formatted summary of regional results."""
    
    print("\n" + "="*80)
    print("REGIONAL COMPARISON SUMMARY")
    print("="*80)
    
    # ----- H1 Summary -----
    print("\n" + "-"*60)
    print("H1: Winter Precipitation Compensation")
    print("-"*60)
    print(f"{'Region':<15} {'Correlation':>12} {'Sample Size':>12} {'Compensation':>12}")
    print("-"*60)
    
    h1_data = all_results.get("H1_winter_precipitation", {})
    for region in sorted(h1_data.keys()):
        data = h1_data[region]
        if "error" in data:
            print(f"{region:<15} {'Error':>12}")
        else:
            corr = data.get("correlation")
            n = data.get("sample_size")
            comp = data.get("compensation_rate")
            print(f"{region:<15} {corr:>12.4f} {n:>12,} {comp:>11.1f}%" if corr else f"{region:<15} {'N/A':>12}")
    
    # ----- H2 Summary -----
    print("\n" + "-"*60)
    print("H2: Spring Temperature → Summer Heat Waves")
    print("-"*60)
    print(f"{'Region':<15} {'Correlation':>12} {'HW (Warm)':>12} {'HW (Cool)':>12}")
    print("-"*60)
    
    h2_data = all_results.get("H2_spring_summer_heat", {})
    for region in sorted(h2_data.keys()):
        data = h2_data[region]
        if "error" in data:
            print(f"{region:<15} {'Error':>12}")
        else:
            corr = data.get("correlation")
            warm = data.get("avg_hw_warm")
            cool = data.get("avg_hw_cool")
            if corr:
                print(f"{region:<15} {corr:>12.4f} {warm:>12.1f} {cool:>12.1f}")
            else:
                print(f"{region:<15} {'N/A':>12}")
    
    # ----- H3 Summary -----
    print("\n" + "-"*60)
    print("H3: Wind → Temperature Moderation")
    print("-"*60)
    print(f"{'Region':<15} {'Mod. Rate':>12} {'Δ Range':>12} {'Events':>12}")
    print("-"*60)
    
    h3_data = all_results.get("H3_wind_moderation", {})
    for region in sorted(h3_data.keys()):
        data = h3_data[region]
        if "error" in data:
            print(f"{region:<15} {'No wind data':>12}")
        else:
            rate = data.get("moderation_rate")
            change = data.get("avg_range_change")
            events = data.get("num_events")
            if rate:
                print(f"{region:<15} {rate:>11.1f}% {change:>12.2f} {events:>12,}")
            else:
                print(f"{region:<15} {'N/A':>12}")
    
    # ----- H4 Summary -----
    print("\n" + "-"*60)
    print("H4: Snowmelt → Spring Precipitation")
    print("-"*60)
    print(f"{'Region':<15} {'Correlation':>12} {'Sample Size':>12} {'Early→Dry':>12}")
    print("-"*60)
    
    h4_data = all_results.get("H4_snowmelt_precipitation", {})
    for region in sorted(h4_data.keys()):
        data = h4_data[region]
        if "error" in data:
            print(f"{region:<15} {'No snow':>12}")
        else:
            corr = data.get("correlation")
            n = data.get("sample_size")
            early = data.get("early_melt_prcp")
            if corr:
                print(f"{region:<15} {corr:>12.4f} {n:>12,} {early:>12.2f}σ")
            else:
                print(f"{region:<15} {'N/A':>12}")
    
    # ----- Overall interpretation -----
    print("\n" + "="*80)
    print("INTERPRETATION")
    print("="*80)
    
    print("""
Key observations:
- H1 (Winter Precip): Compare correlation signs across regions
- H2 (Heat Waves): Southwest/West typically show stronger spring-summer link
- H3 (Wind): Coastal regions may show different patterns than inland
- H4 (Snowmelt): Only applicable to regions with significant snowfall
  (Northeast, Midwest, West mountains, Alaska)

Hypothesis 5 is SUPPORTED if correlations are consistent in direction
(all positive or all negative) across most regions, even if magnitudes differ.

Hypothesis 5 is NOT SUPPORTED if different regions show opposite patterns
(positive correlation in some regions, negative in others).
""")

# MYSQL EXPORT FOR REGIONAL RESULTS

def export_regional_results_to_mysql(all_results, exporter):
    """
    Export regional comparison results to MySQL.
    
    Args:
        all_results: Dict from analyze_regional_consistency()
        exporter: MySQLExporter instance
    """
    print("Exporting regional results to MySQL...")
    
    sql = """
        INSERT INTO regional_results
        (hypothesis, region, correlation, sample_size, effect_size, significant)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            correlation = VALUES(correlation),
            sample_size = VALUES(sample_size),
            effect_size = VALUES(effect_size),
            significant = VALUES(significant)
    """
    
    data = []
    
    for hypothesis, regions in all_results.items():
        for region, metrics in regions.items():
            if "error" in metrics:
                continue
            
            corr = metrics.get("correlation")
            n = metrics.get("sample_size") or metrics.get("num_events")
            effect = metrics.get("moderation_rate") or metrics.get("compensation_rate")
            
            # Determine significance (rough: |corr| > 0.1 or rate > 55%)
            significant = False
            if corr and abs(corr) > 0.1:
                significant = True
            if effect and effect > 55:
                significant = True
            
            data.append((hypothesis, region, corr, n, effect, significant))
    
    if data:
        exporter.execute_many(sql, data)
        print(f"  Exported {len(data)} regional results")

# STANDALONE EXECUTION

if __name__ == "__main__":
    print("Hypothesis 5: Regional Consistency Analysis")
    print("="*50)
    print("\nThis module requires:")
    print("  1. Climate data DataFrame")
    print("  2. Station metadata DataFrame with STATE column")
    print("\nUsage:")
    print("  from hypothesis5_regional_consistency import analyze_regional_consistency")
    print("  results = analyze_regional_consistency(climate_df, stations_df)")