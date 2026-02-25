from pyspark.sql.functions import (
    col, avg, sum, count, stddev, corr, when, lit
)


def analyze_winter_precipitation_lag(df, verbose=True):
    """
    Analyze relationship between early and late winter precipitation.
    
    Tests whether below-average Nov-Dec precipitation predicts 
    above-average Feb-Mar precipitation (atmospheric compensation).
    
    Args:
        df: Spark DataFrame with columns [STATION, DATE, ELEMENT, VALUE, year, month]
        verbose: Whether to print progress and results
        
    Returns:
        tuple: (results_dataframe, results_dict)
    """
    
    if verbose:
        print("\n" + "="*80)
        print("HYPOTHESIS 1: Dry Early Winter → Wet Late Winter")
        print("="*80)
    
    # -------------------------------------------------------------------------
    # Step 1: Filter to precipitation data only
    # -------------------------------------------------------------------------
    prcp_df = df.filter(col("ELEMENT") == "PRCP")
    
    if verbose:
        prcp_count = prcp_df.count()
        print(f"\nPrecipitation records: {prcp_count:,}")
    
    # -------------------------------------------------------------------------
    # Step 2: Calculate early winter precipitation (November-December)
    # -------------------------------------------------------------------------
    early_winter = prcp_df.filter(col("month").isin([11, 12])) \
        .groupBy("STATION", "year") \
        .agg(
            sum("VALUE").alias("early_winter_prcp"),
            count("*").alias("early_winter_days")
        ) \
        .filter(col("early_winter_days") >= 50)  # Quality: need ~80% coverage (61 days total)
    
    if verbose:
        early_count = early_winter.count()
        print(f"Early winter (Nov-Dec) station-years with sufficient data: {early_count:,}")
    
    # -------------------------------------------------------------------------
    # Step 3: Calculate late winter precipitation (February-March)
    # -------------------------------------------------------------------------
    late_winter = prcp_df.filter(col("month").isin([2, 3])) \
        .groupBy("STATION", "year") \
        .agg(
            sum("VALUE").alias("late_winter_prcp"),
            count("*").alias("late_winter_days")
        ) \
        .filter(col("late_winter_days") >= 50)  # Quality: need ~80% coverage (59 days total)
    
    if verbose:
        late_count = late_winter.count()
        print(f"Late winter (Feb-Mar) station-years with sufficient data: {late_count:,}")
    
    # -------------------------------------------------------------------------
    # Step 4: Join early and late winter data
    # Note: Late winter of year N corresponds to early winter of year N-1
    # So we adjust early winter year by +1 to align with same winter season
    # -------------------------------------------------------------------------
    early_winter_adj = early_winter.withColumn("winter_year", col("year") + 1)
    
    winter_combined = early_winter_adj.alias("early").join(
        late_winter.alias("late"),
        (col("early.STATION") == col("late.STATION")) & 
        (col("early.winter_year") == col("late.year")),
        "inner"
    ).select(
        col("early.STATION").alias("STATION"),
        col("early.winter_year").alias("year"),
        col("early.early_winter_prcp"),
        col("late.late_winter_prcp")
    )
    
    if verbose:
        combined_count = winter_combined.count()
        print(f"Matched winter seasons (early + late): {combined_count:,}")
    
    # -------------------------------------------------------------------------
    # Step 5: Calculate station-level baselines (long-term averages)
    # -------------------------------------------------------------------------
    station_baselines = winter_combined.groupBy("STATION") \
        .agg(
            avg("early_winter_prcp").alias("baseline_early"),
            avg("late_winter_prcp").alias("baseline_late"),
            stddev("early_winter_prcp").alias("std_early"),
            stddev("late_winter_prcp").alias("std_late"),
            count("*").alias("num_years")
        ) \
        .filter(col("num_years") >= 5)  # Need enough years for reliable baseline
    
    # -------------------------------------------------------------------------
    # Step 6: Calculate standardized anomalies
    # Anomaly = (observed - baseline) / std_dev
    # -------------------------------------------------------------------------
    with_anomalies = winter_combined.join(station_baselines, "STATION") \
        .withColumn(
            "early_anomaly",
            (col("early_winter_prcp") - col("baseline_early")) / col("std_early")
        ) \
        .withColumn(
            "late_anomaly",
            (col("late_winter_prcp") - col("baseline_late")) / col("std_late")
        ) \
        .filter(col("std_early") > 0)  # Avoid division by zero
        
    # -------------------------------------------------------------------------
    # Step 7: Calculate correlation
    # -------------------------------------------------------------------------
    correlation_result = with_anomalies.select(
        corr("early_anomaly", "late_anomaly").alias("correlation")
    ).collect()[0]["correlation"]
    
    if verbose:
        print(f"\nCorrelation (early winter deficit vs late winter surplus): {correlation_result:.4f}")
    
    # -------------------------------------------------------------------------
    # Step 8: Test hypothesis - Do dry early winters predict wet late winters?
    # -------------------------------------------------------------------------
    # Dry early winter = more than 1 standard deviation below average
    dry_early = with_anomalies.filter(col("early_anomaly") < -1)
    
    # Count how many have above-average late winter
    wet_late_given_dry_early = dry_early.filter(col("late_anomaly") > 0).count()
    total_dry_early = dry_early.count()
    
    compensation_rate = None
    if total_dry_early > 0:
        compensation_rate = wet_late_given_dry_early / total_dry_early * 100
        if verbose:
            print(f"\nWhen early winter is dry (>1σ below avg):")
            print(f"  Total cases: {total_dry_early}")
            print(f"  Cases with above-avg late winter: {wet_late_given_dry_early}")
            print(f"  Compensation rate: {compensation_rate:.1f}%")
    
    # -------------------------------------------------------------------------
    # Step 9: Additional analysis - wet early winter
    # -------------------------------------------------------------------------
    wet_early = with_anomalies.filter(col("early_anomaly") > 1)
    dry_late_given_wet_early = wet_early.filter(col("late_anomaly") < 0).count()
    total_wet_early = wet_early.count()
    
    reverse_rate = None
    if total_wet_early > 0:
        reverse_rate = dry_late_given_wet_early / total_wet_early * 100
        if verbose:
            print(f"\nWhen early winter is wet (>1σ above avg):")
            print(f"  Total cases: {total_wet_early}")
            print(f"  Cases with below-avg late winter: {dry_late_given_wet_early}")
            print(f"  Reverse compensation rate: {reverse_rate:.1f}%")
    
    # -------------------------------------------------------------------------
    # Compile results
    # -------------------------------------------------------------------------
    results = {
        "correlation": correlation_result,
        "sample_size": with_anomalies.count(),
        "compensation_rate": compensation_rate,
        "reverse_compensation_rate": reverse_rate,
        "dry_early_cases": total_dry_early,
        "wet_early_cases": total_wet_early
    }
    
    if verbose:
        print("\n" + "-"*40)
        interpretation = "SUPPORTED" if (correlation_result and correlation_result < -0.1) else "NOT SUPPORTED"
        print(f"Hypothesis interpretation: {interpretation}")
        if correlation_result and correlation_result < -0.1:
            print("  Negative correlation suggests atmospheric compensation exists")
        else:
            print("  No strong evidence of systematic compensation pattern")
    
    return with_anomalies, results


def get_regional_winter_precipitation(df, station_regions_df):
    """
    Run winter precipitation analysis by region.
    
    Args:
        df: Climate data DataFrame
        station_regions_df: DataFrame with STATION and REGION columns
        
    Returns:
        dict: Results by region
    """
    # Join with regions
    df_with_region = df.join(
        station_regions_df.select("STATION", "REGION"),
        "STATION",
        "left"
    )
    
    regions = df_with_region.select("REGION").distinct().collect()
    regional_results = {}
    
    for row in regions:
        region = row["REGION"]
        if region is None:
            continue
            
        region_df = df_with_region.filter(col("REGION") == region)
        _, results = analyze_winter_precipitation_lag(region_df, verbose=False)
        regional_results[region] = results
        print(f"  {region}: correlation = {results['correlation']:.4f}")
    
    return regional_results