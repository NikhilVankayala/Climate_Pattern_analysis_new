from pyspark.sql.functions import (
    col, avg, sum, count, stddev, when, lag, lead, expr, lit
)
from pyspark.sql.window import Window


def pivot_to_wide_format(df):
    """
    Convert long format climate data to wide format for multi-variable analysis.
    
    Args:
        df: DataFrame in long format [STATION, DATE, ELEMENT, VALUE]
        
    Returns:
        DataFrame in wide format [STATION, DATE, TMAX, TMIN, AWND, ...]
    """
    # Extract each element as separate DataFrame
    tmax_df = df.filter(col("ELEMENT") == "TMAX") \
        .select("STATION", "DATE", col("VALUE").alias("TMAX"))
    
    tmin_df = df.filter(col("ELEMENT") == "TMIN") \
        .select("STATION", "DATE", col("VALUE").alias("TMIN"))
    
    awnd_df = df.filter(col("ELEMENT") == "AWND") \
        .select("STATION", "DATE", col("VALUE").alias("AWND"))
    
    # Join into wide format
    # Inner join ensures we only keep days with all three measurements
    wide_df = tmax_df \
        .join(tmin_df, ["STATION", "DATE"], "inner") \
        .join(awnd_df, ["STATION", "DATE"], "inner")
    
    return wide_df


def analyze_wind_temperature_moderation(df, lag_days=7, verbose=True):
    """
    Analyze whether high wind speeds lead to temperature moderation.
    
    Tests whether the diurnal temperature range (TMAX - TMIN) decreases
    in the days following high wind events.
    
    Args:
        df: Spark DataFrame with columns [STATION, DATE, ELEMENT, VALUE, year, month]
        lag_days: Number of days to look before/after wind events (default 7)
        verbose: Whether to print progress and results
        
    Returns:
        tuple: (results_dataframe, results_dict)
    """
    
    if verbose:
        print("\n" + "="*80)
        print("HYPOTHESIS 3: High Wind → Temperature Moderation")
        print("="*80)
    
    # -------------------------------------------------------------------------
    # Step 1: Check wind data availability
    # -------------------------------------------------------------------------
    awnd_count = df.filter(col("ELEMENT") == "AWND").count()
    total_count = df.count()
    
    if verbose:
        print(f"\nWind (AWND) records: {awnd_count:,}")
        print(f"Total records: {total_count:,}")
        print(f"Wind coverage: {awnd_count/total_count*100:.1f}%")
    
    if awnd_count < 10000:
        print("\nWARNING: Limited wind data. Results may not be reliable.")
    
    # -------------------------------------------------------------------------
    # Step 2: Pivot to wide format (need TMAX, TMIN, AWND together)
    # -------------------------------------------------------------------------
    daily_df = pivot_to_wide_format(df)
    
    if verbose:
        daily_count = daily_df.count()
        print(f"Days with TMAX + TMIN + AWND: {daily_count:,}")
    
    if daily_df.count() == 0:
        print("ERROR: No data with all three variables (TMAX, TMIN, AWND)")
        return None, {"error": "No overlapping data"}
    
    # -------------------------------------------------------------------------
    # Step 3: Calculate diurnal temperature range
    # -------------------------------------------------------------------------
    daily_df = daily_df.withColumn("temp_range", col("TMAX") - col("TMIN"))
    
    # Filter out invalid ranges (should be positive)
    daily_df = daily_df.filter(col("temp_range") > 0)
    
    if verbose:
        avg_range = daily_df.select(avg("temp_range")).collect()[0][0]
        print(f"Average diurnal temp range: {avg_range:.2f}°C")
    
    # -------------------------------------------------------------------------
    # Step 4: Identify high wind days (>90th percentile per station)
    # -------------------------------------------------------------------------
    wind_thresholds = daily_df.groupBy("STATION") \
        .agg(
            expr("percentile(AWND, 0.90)").alias("wind_threshold"),
            avg("AWND").alias("avg_wind"),
            count("*").alias("total_days")
        ) \
        .filter(col("total_days") >= 100)  # Need enough data for reliable percentile
    
    daily_with_threshold = daily_df.join(wind_thresholds, "STATION") \
        .withColumn("is_high_wind", col("AWND") > col("wind_threshold"))
    
    if verbose:
        high_wind_days = daily_with_threshold.filter(col("is_high_wind")).count()
        total_days = daily_with_threshold.count()
        print(f"High wind days (>90th percentile): {high_wind_days:,} / {total_days:,}")
    
    # -------------------------------------------------------------------------
    # Step 5: Calculate lagged temperature ranges using window functions
    # -------------------------------------------------------------------------
    window = Window.partitionBy("STATION").orderBy("DATE")
    
    # Add lag columns (days before the event)
    with_lags = daily_with_threshold
    for i in range(1, lag_days + 1):
        with_lags = with_lags.withColumn(
            f"temp_range_lag_{i}", 
            lag("temp_range", i).over(window)
        )
    
    # Add lead columns (days after the event)
    for i in range(1, lag_days + 1):
        with_lags = with_lags.withColumn(
            f"temp_range_lead_{i}", 
            lead("temp_range", i).over(window)
        )
    
    # -------------------------------------------------------------------------
    # Step 6: Calculate average temp range before and after
    # -------------------------------------------------------------------------
    # Build column references for averaging
    lag_cols = [col(f"temp_range_lag_{i}") for i in range(1, lag_days + 1)]
    lead_cols = [col(f"temp_range_lead_{i}") for i in range(1, lag_days + 1)]
    
    # Average of lag days (before)
    with_lags = with_lags.withColumn(
        "avg_range_before",
        sum(lag_cols) / lag_days
    )
    
    # Average of lead days (after)
    with_lags = with_lags.withColumn(
        "avg_range_after",
        sum(lead_cols) / lag_days
    )
    
    # -------------------------------------------------------------------------
    # Step 7: Filter to high wind days with complete before/after data
    # -------------------------------------------------------------------------
    high_wind_events = with_lags.filter(
        col("is_high_wind") & 
        col("avg_range_before").isNotNull() & 
        col("avg_range_after").isNotNull()
    )
    
    # Calculate change in temperature range
    high_wind_events = high_wind_events.withColumn(
        "range_change",
        col("avg_range_after") - col("avg_range_before")
    )
    
    if verbose:
        event_count = high_wind_events.count()
        print(f"High wind events with complete before/after data: {event_count:,}")
    
    # -------------------------------------------------------------------------
    # Step 8: Aggregate results
    # -------------------------------------------------------------------------
    if high_wind_events.count() == 0:
        print("ERROR: No complete high wind events to analyze")
        return None, {"error": "No complete events"}
    
    results_agg = high_wind_events.agg(
        avg("avg_range_before").alias("avg_range_before"),
        avg("avg_range_after").alias("avg_range_after"),
        avg("range_change").alias("avg_range_change"),
        stddev("range_change").alias("std_range_change"),
        count("*").alias("num_events")
    ).collect()[0]
    
    if verbose:
        print(f"\nResults:")
        print(f"  Average temp range BEFORE high wind: {results_agg['avg_range_before']:.2f}°C")
        print(f"  Average temp range AFTER high wind: {results_agg['avg_range_after']:.2f}°C")
        print(f"  Average change: {results_agg['avg_range_change']:.2f}°C")
    
    # -------------------------------------------------------------------------
    # Step 9: Calculate moderation rate
    # -------------------------------------------------------------------------
    moderation_count = high_wind_events.filter(col("range_change") < 0).count()
    total_events = results_agg['num_events']
    
    moderation_rate = None
    if total_events > 0:
        moderation_rate = moderation_count / total_events * 100
        if verbose:
            print(f"\nEvents showing moderation (decreased range): {moderation_count:,} / {total_events:,}")
            print(f"Moderation rate: {moderation_rate:.1f}%")
    
    # -------------------------------------------------------------------------
    # Step 10: Statistical significance check
    # -------------------------------------------------------------------------
    # Compare to random expectation (50% would show decrease by chance)
    expected_by_chance = 50.0
    
    if verbose:
        print(f"\nStatistical interpretation:")
        print(f"  Expected by chance: {expected_by_chance:.1f}%")
        if moderation_rate:
            diff = moderation_rate - expected_by_chance
            print(f"  Observed - Expected: {diff:+.1f}%")
    
    # -------------------------------------------------------------------------
    # Compile results
    # -------------------------------------------------------------------------
    results = {
        "avg_range_before": results_agg['avg_range_before'],
        "avg_range_after": results_agg['avg_range_after'],
        "avg_range_change": results_agg['avg_range_change'],
        "std_range_change": results_agg['std_range_change'],
        "num_events": results_agg['num_events'],
        "moderation_rate": moderation_rate,
        "lag_days": lag_days
    }
    
    if verbose:
        print("\n" + "-"*40)
        # Hypothesis supported if moderation rate > 55% (noticeably above chance)
        interpretation = "SUPPORTED" if (moderation_rate and moderation_rate > 55) else "NOT SUPPORTED"
        print(f"Hypothesis interpretation: {interpretation}")
        if moderation_rate and moderation_rate > 55:
            print("  Wind events lead to temperature moderation more often than chance")
        else:
            print("  No clear evidence that wind leads to temperature moderation")
    
    return high_wind_events, results


def analyze_wind_by_season(df, verbose=True):
    """
    Analyze wind-temperature relationship by season.
    
    Args:
        df: Climate data DataFrame
        verbose: Print results
        
    Returns:
        dict: Results by season
    """
    seasonal_results = {}
    
    seasons = {
        "Winter": [12, 1, 2],
        "Spring": [3, 4, 5],
        "Summer": [6, 7, 8],
        "Fall": [9, 10, 11]
    }
    
    for season_name, months in seasons.items():
        if verbose:
            print(f"\n--- {season_name} ---")
        
        season_df = df.filter(col("month").isin(months))
        _, results = analyze_wind_temperature_moderation(season_df, verbose=False)
        
        if results.get("error"):
            seasonal_results[season_name] = {"error": results["error"]}
        else:
            seasonal_results[season_name] = results
            if verbose:
                print(f"  Moderation rate: {results.get('moderation_rate', 'N/A')}")
                print(f"  Avg range change: {results.get('avg_range_change', 'N/A')}")
    
    return seasonal_results