from pyspark.sql.functions import (
    col, avg, sum, count, stddev, corr, when, max, min, expr, dayofyear
)


def find_snowmelt_dates(df):
    """
    Find the snowmelt date for each station-year.
    
    Snowmelt date = last day of winter/spring with snow depth > 0
    
    Args:
        df: DataFrame with SNWD data [STATION, DATE, ELEMENT, VALUE, year, month]
        
    Returns:
        DataFrame with [STATION, year, snowmelt_doy, snow_days]
    """
    
    # Filter to snow depth data
    snwd_df = df.filter(col("ELEMENT") == "SNWD")
    
    # Add day of year column
    # Convert DATE (YYYYMMDD integer) to actual date, then get day of year
    snwd_df = snwd_df.withColumn(
        "day_of_year",
        dayofyear(expr("to_date(cast(DATE as string), 'yyyyMMdd')"))
    )
    
    # Focus on winter/spring months (Jan-May) when snowmelt typically occurs
    # Exclude fall/early winter to avoid counting new snow season
    winter_spring_snow = snwd_df.filter(
        col("month").isin([1, 2, 3, 4, 5]) & 
        (col("VALUE") > 0)  # Days with actual snow on ground
    )
    
    # Find last snow day per station-year
    last_snow_day = winter_spring_snow.groupBy("STATION", "year") \
        .agg(
            max("day_of_year").alias("snowmelt_doy"),
            count("*").alias("snow_days"),
            max("VALUE").alias("max_snow_depth"),
            avg("VALUE").alias("avg_snow_depth")
        )
    
    return last_snow_day


def analyze_snowmelt_spring_precipitation(df, verbose=True):
    """
    Analyze relationship between snowmelt timing and spring precipitation.
    
    Tests whether earlier snowmelt predicts below-average April-May 
    precipitation (both indicating persistent dry conditions).
    
    Args:
        df: Spark DataFrame with columns [STATION, DATE, ELEMENT, VALUE, year, month]
        verbose: Whether to print progress and results
        
    Returns:
        tuple: (results_dataframe, results_dict)
    """
    
    if verbose:
        print("\n" + "="*80)
        print("HYPOTHESIS 4: Early Snowmelt → Reduced Spring Rainfall")
        print("="*80)
    
    # -------------------------------------------------------------------------
    # Step 1: Check snow data availability
    # -------------------------------------------------------------------------
    snwd_count = df.filter(col("ELEMENT") == "SNWD").count()
    
    if verbose:
        print(f"\nSnow depth (SNWD) records: {snwd_count:,}")
    
    if snwd_count < 1000:
        print("WARNING: Very limited snow data. Results may not be meaningful.")
    
    # -------------------------------------------------------------------------
    # Step 2: Find snowmelt dates
    # -------------------------------------------------------------------------
    last_snow_day = find_snowmelt_dates(df)
    
    # Filter to station-years with meaningful snow coverage
    # Need at least 10 days with snow to be a "snow station"
    last_snow_day = last_snow_day.filter(col("snow_days") >= 10)
    
    if verbose:
        snow_records = last_snow_day.count()
        snow_stations = last_snow_day.select("STATION").distinct().count()
        print(f"Station-years with sufficient snow data: {snow_records:,}")
        print(f"Unique stations with snow: {snow_stations:,}")
    
    if last_snow_day.count() == 0:
        print("ERROR: No stations with sufficient snow data")
        return None, {"error": "No snow data"}
    
    # -------------------------------------------------------------------------
    # Step 3: Calculate snowmelt baseline and anomaly per station
    # -------------------------------------------------------------------------
    snowmelt_baseline = last_snow_day.groupBy("STATION") \
        .agg(
            avg("snowmelt_doy").alias("baseline_melt_doy"),
            stddev("snowmelt_doy").alias("std_melt_doy"),
            count("*").alias("years_with_snow"),
            avg("snow_days").alias("avg_snow_days")
        ) \
        .filter(
            (col("years_with_snow") >= 5) &  # Need enough years
            (col("std_melt_doy") > 0)         # Need variation
        )
    
    if verbose:
        baseline_stations = snowmelt_baseline.count()
        print(f"Stations with reliable snowmelt baselines: {baseline_stations:,}")
    
    # Calculate snowmelt anomaly
    # Negative = early melt, Positive = late melt
    snowmelt_with_anomaly = last_snow_day.join(snowmelt_baseline, "STATION") \
        .withColumn(
            "melt_anomaly_days",
            col("snowmelt_doy") - col("baseline_melt_doy")
        ) \
        .withColumn(
            "melt_anomaly_std",
            (col("snowmelt_doy") - col("baseline_melt_doy")) / col("std_melt_doy")
        )
    
    if verbose:
        avg_melt = snowmelt_with_anomaly.select(avg("snowmelt_doy")).collect()[0][0]
        print(f"Average snowmelt day of year: {avg_melt:.0f} (~{int(avg_melt/30)+1} month)")
    
    # -------------------------------------------------------------------------
    # Step 4: Calculate spring precipitation (April-May)
    # -------------------------------------------------------------------------
    spring_prcp = df.filter(
        (col("ELEMENT") == "PRCP") & 
        col("month").isin([4, 5])
    ).groupBy("STATION", "year") \
        .agg(
            sum("VALUE").alias("spring_prcp"),
            count("*").alias("spring_prcp_days"),
            avg("VALUE").alias("avg_daily_prcp")
        ) \
        .filter(col("spring_prcp_days") >= 50)  # Need ~80% coverage (61 days)
    
    if verbose:
        prcp_records = spring_prcp.count()
        print(f"Station-years with spring precipitation data: {prcp_records:,}")
    
    # -------------------------------------------------------------------------
    # Step 5: Calculate spring precipitation baseline and anomaly
    # -------------------------------------------------------------------------
    prcp_baseline = spring_prcp.groupBy("STATION") \
        .agg(
            avg("spring_prcp").alias("baseline_spring_prcp"),
            stddev("spring_prcp").alias("std_spring_prcp"),
            count("*").alias("prcp_years")
        ) \
        .filter(
            (col("prcp_years") >= 5) &
            (col("std_spring_prcp") > 0)
        )
    
    spring_prcp_with_anomaly = spring_prcp.join(prcp_baseline, "STATION") \
        .withColumn(
            "prcp_anomaly_mm",
            col("spring_prcp") - col("baseline_spring_prcp")
        ) \
        .withColumn(
            "prcp_anomaly_std",
            (col("spring_prcp") - col("baseline_spring_prcp")) / col("std_spring_prcp")
        )
    
    # -------------------------------------------------------------------------
    # Step 6: Join snowmelt and precipitation data
    # -------------------------------------------------------------------------
    combined = snowmelt_with_anomaly.join(
        spring_prcp_with_anomaly,
        ["STATION", "year"],
        "inner"
    )
    
    if verbose:
        combined_count = combined.count()
        combined_stations = combined.select("STATION").distinct().count()
        print(f"Combined snowmelt + precipitation records: {combined_count:,}")
        print(f"Stations in combined analysis: {combined_stations:,}")
    
    if combined.count() == 0:
        print("ERROR: No overlapping snowmelt and precipitation data")
        return None, {"error": "No overlapping data"}
    
    # -------------------------------------------------------------------------
    # Step 7: Calculate correlations
    # -------------------------------------------------------------------------
    # Raw correlation: melt day vs spring precipitation
    correlation = combined.select(
        corr("snowmelt_doy", "spring_prcp")
    ).collect()[0][0]
    
    # Standardized correlation: melt anomaly vs precip anomaly
    corr_std = combined.select(
        corr("melt_anomaly_std", "prcp_anomaly_std")
    ).collect()[0][0]
    
    # Correlation with days anomaly
    corr_days = combined.select(
        corr("melt_anomaly_days", "spring_prcp")
    ).collect()[0][0]
    
    if verbose:
        print(f"\nCorrelations:")
        print(f"  Snowmelt DOY vs Spring precipitation: {correlation:.4f}")
        print(f"  Melt anomaly (days) vs Spring precip: {corr_days:.4f}")
        print(f"  Melt anomaly (std) vs Precip anomaly (std): {corr_std:.4f}")
    
    # -------------------------------------------------------------------------
    # Step 8: Compare early vs late melt years
    # -------------------------------------------------------------------------
    # Early melt: >1 standard deviation early
    early_melt = combined.filter(col("melt_anomaly_std") < -1)
    # Late melt: >1 standard deviation late
    late_melt = combined.filter(col("melt_anomaly_std") > 1)
    
    early_melt_count = early_melt.count()
    late_melt_count = late_melt.count()
    
    early_melt_prcp = None
    late_melt_prcp = None
    
    if early_melt_count > 0:
        early_melt_prcp = early_melt.select(avg("prcp_anomaly_std")).collect()[0][0]
    
    if late_melt_count > 0:
        late_melt_prcp = late_melt.select(avg("prcp_anomaly_std")).collect()[0][0]
    
    if verbose:
        print(f"\nComparison by snowmelt timing:")
        print(f"  Early melt years (>1σ early): {early_melt_count:,}")
        if early_melt_prcp is not None:
            print(f"    Avg spring precip anomaly: {early_melt_prcp:+.2f}σ")
        print(f"  Late melt years (>1σ late): {late_melt_count:,}")
        if late_melt_prcp is not None:
            print(f"    Avg spring precip anomaly: {late_melt_prcp:+.2f}σ")
    
    # -------------------------------------------------------------------------
    # Step 9: Calculate percentage showing expected pattern
    # -------------------------------------------------------------------------
    # Early melt should correlate with dry spring
    early_dry = early_melt.filter(col("prcp_anomaly_std") < 0).count()
    early_dry_rate = (early_dry / early_melt_count * 100) if early_melt_count > 0 else None
    
    # Late melt should correlate with wet spring
    late_wet = late_melt.filter(col("prcp_anomaly_std") > 0).count()
    late_wet_rate = (late_wet / late_melt_count * 100) if late_melt_count > 0 else None
    
    if verbose:
        if early_dry_rate:
            print(f"\nEarly melt years with below-avg spring rain: {early_dry_rate:.1f}%")
        if late_wet_rate:
            print(f"Late melt years with above-avg spring rain: {late_wet_rate:.1f}%")
    
    # -------------------------------------------------------------------------
    # Compile results
    # -------------------------------------------------------------------------
    results = {
        "correlation": correlation,
        "correlation_standardized": corr_std,
        "correlation_days": corr_days,
        "early_melt_prcp_anomaly": early_melt_prcp,
        "late_melt_prcp_anomaly": late_melt_prcp,
        "early_melt_cases": early_melt_count,
        "late_melt_cases": late_melt_count,
        "early_dry_rate": early_dry_rate,
        "late_wet_rate": late_wet_rate,
        "sample_size": combined.count(),
        "num_stations": combined.select("STATION").distinct().count()
    }
    
    if verbose:
        print("\n" + "-"*40)
        # Positive correlation expected (later melt = more precip)
        interpretation = "SUPPORTED" if (correlation and correlation > 0.1) else "NOT SUPPORTED"
        print(f"Hypothesis interpretation: {interpretation}")
        if correlation and correlation > 0.1:
            print("  Positive correlation confirms early melt predicts dry spring")
        else:
            print("  No clear relationship between snowmelt timing and spring precipitation")
    
    return combined, results


def get_snowmelt_trends(df):
    """
    Analyze trends in snowmelt timing over time.
    
    Args:
        df: Climate data DataFrame
        
    Returns:
        DataFrame with yearly snowmelt statistics
    """
    last_snow_day = find_snowmelt_dates(df)
    
    # Aggregate by year
    yearly_trends = last_snow_day \
        .groupBy("year") \
        .agg(
            avg("snowmelt_doy").alias("avg_melt_doy"),
            stddev("snowmelt_doy").alias("std_melt_doy"),
            min("snowmelt_doy").alias("earliest_melt"),
            max("snowmelt_doy").alias("latest_melt"),
            count("*").alias("num_stations")
        ) \
        .orderBy("year")
    
    return yearly_trends


def analyze_by_elevation(df, station_metadata_df, verbose=True):
    """
    Analyze snowmelt-precipitation relationship by elevation bands.
    
    Args:
        df: Climate data DataFrame
        station_metadata_df: DataFrame with STATION and ELEVATION columns
        verbose: Print results
        
    Returns:
        dict: Results by elevation band
    """
    # Join with station metadata
    df_with_elev = df.join(
        station_metadata_df.select("STATION", "ELEVATION"),
        "STATION",
        "left"
    ).filter(col("ELEVATION").isNotNull())
    
    # Define elevation bands
    df_with_elev = df_with_elev.withColumn(
        "elev_band",
        when(col("ELEVATION") < 500, "Low (<500m)")
        .when(col("ELEVATION") < 1500, "Mid (500-1500m)")
        .otherwise("High (>1500m)")
    )
    
    elevation_results = {}
    
    for band in ["Low (<500m)", "Mid (500-1500m)", "High (>1500m)"]:
        if verbose:
            print(f"\n--- {band} ---")
        
        band_df = df_with_elev.filter(col("elev_band") == band)
        _, results = analyze_snowmelt_spring_precipitation(band_df, verbose=False)
        
        if results.get("error"):
            elevation_results[band] = {"error": results["error"]}
        else:
            elevation_results[band] = results
            if verbose:
                print(f"  Correlation: {results.get('correlation', 'N/A')}")
                print(f"  Sample size: {results.get('sample_size', 'N/A')}")
    
    return elevation_results