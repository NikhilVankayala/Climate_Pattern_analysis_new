from pyspark.sql.functions import (
    col, avg, sum, count, stddev, corr, when, lag, lead, max, min, expr
)
from pyspark.sql.window import Window


def detect_heat_waves(df, threshold_percentile=95):
    """
    Detect heat wave events in summer temperature data.
    
    Heat wave definition: 3+ consecutive days where TMAX exceeds
    the station's threshold percentile.
    
    Args:
        df: DataFrame with summer TMAX data [STATION, DATE, VALUE, year, month]
        threshold_percentile: Percentile for defining "extreme" (default 95)
        
    Returns:
        DataFrame with heat wave day flags
    """
    
    # Calculate threshold per station (e.g., 95th percentile of summer temps)
    thresholds = df.groupBy("STATION") \
        .agg(
            expr(f"percentile(VALUE, {threshold_percentile/100})").alias("heat_threshold")
        )
    
    # Join thresholds and flag extreme days
    with_threshold = df.join(thresholds, "STATION") \
        .withColumn("is_extreme", col("VALUE") > col("heat_threshold"))
    
    # Use window function to look at neighboring days
    window = Window.partitionBy("STATION", "year").orderBy("DATE")
    
    # Get extreme status of surrounding days
    consecutive = with_threshold \
        .withColumn("prev1_extreme", lag("is_extreme", 1).over(window)) \
        .withColumn("prev2_extreme", lag("is_extreme", 2).over(window)) \
        .withColumn("next1_extreme", lead("is_extreme", 1).over(window)) \
        .withColumn("next2_extreme", lead("is_extreme", 2).over(window))
    
    # A day is part of a heat wave if it's in a sequence of 3+ extreme days
    # This includes being at the start, middle, or end of such a sequence
    heat_wave_days = consecutive.withColumn(
        "is_heat_wave_day",
        # Middle of 3-day sequence (extreme day with extreme neighbors)
        (col("is_extreme") & col("prev1_extreme") & col("next1_extreme")) |
        # Start of 3-day sequence (extreme with 2 extreme days following)
        (col("is_extreme") & col("next1_extreme") & col("next2_extreme")) |
        # End of 3-day sequence (extreme with 2 extreme days before)
        (col("is_extreme") & col("prev1_extreme") & col("prev2_extreme"))
    )
    
    return heat_wave_days


def analyze_spring_summer_heat(df, heat_threshold_percentile=95, verbose=True):
    """
    Analyze relationship between spring temperatures and summer heat waves.
    
    Tests whether warmer-than-average springs lead to more frequent
    or more intense summer heat waves.
    
    Args:
        df: Spark DataFrame with columns [STATION, DATE, ELEMENT, VALUE, year, month]
        heat_threshold_percentile: Percentile for heat wave threshold (default 95)
        verbose: Whether to print progress and results
        
    Returns:
        tuple: (results_dataframe, results_dict)
    """
    
    if verbose:
        print("\n" + "="*80)
        print("HYPOTHESIS 2: Spring Temperatures → Summer Heat Wave Intensity")
        print("="*80)
    
    # -------------------------------------------------------------------------
    # Step 1: Filter to TMAX data only
    # -------------------------------------------------------------------------
    tmax_df = df.filter(col("ELEMENT") == "TMAX")
    
    if verbose:
        tmax_count = tmax_df.count()
        print(f"\nTMAX records: {tmax_count:,}")
    
    # -------------------------------------------------------------------------
    # Step 2: Calculate spring average temperatures (March-May)
    # -------------------------------------------------------------------------
    spring_temps = tmax_df.filter(col("month").isin([3, 4, 5])) \
        .groupBy("STATION", "year") \
        .agg(
            avg("VALUE").alias("spring_avg_tmax"),
            count("*").alias("spring_days"),
            stddev("VALUE").alias("spring_temp_std")
        ) \
        .filter(col("spring_days") >= 75)  # Need ~80% coverage (92 days total)
    
    if verbose:
        spring_count = spring_temps.count()
        print(f"Spring (Mar-May) station-years with sufficient data: {spring_count:,}")
    
    # -------------------------------------------------------------------------
    # Step 3: Calculate spring temperature baselines and anomalies
    # -------------------------------------------------------------------------
    spring_baseline = spring_temps.groupBy("STATION") \
        .agg(
            avg("spring_avg_tmax").alias("baseline_spring_temp"),
            stddev("spring_avg_tmax").alias("std_spring_temp"),
            count("*").alias("num_years")
        ) \
        .filter(col("num_years") >= 5)
    
    spring_with_anomaly = spring_temps.join(spring_baseline, "STATION") \
        .withColumn(
            "spring_temp_anomaly",
            (col("spring_avg_tmax") - col("baseline_spring_temp")) / col("std_spring_temp")
        ) \
        .filter(col("std_spring_temp") > 0)
    
    if verbose:
        stations_analyzed = spring_with_anomaly.select("STATION").distinct().count()
        print(f"Stations with reliable spring baselines: {stations_analyzed:,}")
    
    # -------------------------------------------------------------------------
    # Step 4: Detect summer heat waves (June-August)
    # -------------------------------------------------------------------------
    summer_temps = tmax_df.filter(col("month").isin([6, 7, 8]))
    
    if verbose:
        summer_count = summer_temps.count()
        print(f"Summer (Jun-Aug) TMAX records: {summer_count:,}")
    
    # Detect heat waves
    heat_wave_days = detect_heat_waves(summer_temps, heat_threshold_percentile)
    
    # -------------------------------------------------------------------------
    # Step 5: Aggregate heat wave metrics per station-year
    # -------------------------------------------------------------------------
    heat_wave_metrics = heat_wave_days \
        .groupBy("STATION", "year") \
        .agg(
            # Count of heat wave days
            sum(when(col("is_heat_wave_day"), 1).otherwise(0)).alias("heat_wave_days"),
            # Maximum temperature during heat waves
            max(when(col("is_heat_wave_day"), col("VALUE"))).alias("max_heat_wave_temp"),
            # Average temperature during heat waves
            avg(when(col("is_heat_wave_day"), col("VALUE"))).alias("avg_heat_wave_temp"),
            # Total extreme days (may not be consecutive)
            sum(when(col("is_extreme"), 1).otherwise(0)).alias("extreme_days"),
            # Total summer days analyzed
            count("*").alias("summer_days")
        )
    
    if verbose:
        hw_years = heat_wave_metrics.filter(col("heat_wave_days") > 0).count()
        total_years = heat_wave_metrics.count()
        print(f"Station-years with heat waves: {hw_years:,} / {total_years:,}")
    
    # -------------------------------------------------------------------------
    # Step 6: Join spring temperatures with summer heat wave metrics
    # -------------------------------------------------------------------------
    combined = spring_with_anomaly.join(
        heat_wave_metrics, 
        ["STATION", "year"], 
        "left"
    ).fillna(0, subset=["heat_wave_days", "extreme_days"])
    
    if verbose:
        combined_count = combined.count()
        print(f"Combined spring-summer records: {combined_count:,}")
    
    # -------------------------------------------------------------------------
    # Step 7: Calculate correlations
    # -------------------------------------------------------------------------
    # Correlation: spring temp anomaly vs number of heat wave days
    corr_days = combined.select(
        corr("spring_temp_anomaly", "heat_wave_days")
    ).collect()[0][0]
    
    # Correlation: spring temp anomaly vs max heat wave temperature
    corr_intensity = combined.filter(col("max_heat_wave_temp").isNotNull()) \
        .select(
            corr("spring_temp_anomaly", "max_heat_wave_temp")
        ).collect()[0][0]
    
    # Correlation: spring temp anomaly vs total extreme days
    corr_extreme = combined.select(
        corr("spring_temp_anomaly", "extreme_days")
    ).collect()[0][0]
    
    if verbose:
        print(f"\nCorrelations:")
        print(f"  Spring temp anomaly vs Heat wave days: {corr_days:.4f}")
        print(f"  Spring temp anomaly vs Max heat wave temp: {corr_intensity:.4f if corr_intensity else 'N/A'}")
        print(f"  Spring temp anomaly vs Total extreme days: {corr_extreme:.4f}")
    
    # -------------------------------------------------------------------------
    # Step 8: Compare warm vs cool springs
    # -------------------------------------------------------------------------
    # Warm springs: >1 standard deviation above average
    warm_springs = combined.filter(col("spring_temp_anomaly") > 1)
    # Cool springs: >1 standard deviation below average
    cool_springs = combined.filter(col("spring_temp_anomaly") < -1)
    
    avg_hw_warm = warm_springs.select(avg("heat_wave_days")).collect()[0][0]
    avg_hw_cool = cool_springs.select(avg("heat_wave_days")).collect()[0][0]
    
    avg_extreme_warm = warm_springs.select(avg("extreme_days")).collect()[0][0]
    avg_extreme_cool = cool_springs.select(avg("extreme_days")).collect()[0][0]
    
    warm_count = warm_springs.count()
    cool_count = cool_springs.count()
    
    if verbose:
        print(f"\nComparison by spring temperature:")
        print(f"  Warm springs (>1σ): {warm_count:,} cases")
        print(f"    Avg heat wave days: {avg_hw_warm:.1f}")
        print(f"    Avg extreme days: {avg_extreme_warm:.1f}")
        print(f"  Cool springs (<-1σ): {cool_count:,} cases")
        print(f"    Avg heat wave days: {avg_hw_cool:.1f}")
        print(f"    Avg extreme days: {avg_extreme_cool:.1f}")
        
        if avg_hw_warm and avg_hw_cool and avg_hw_cool > 0:
            ratio = avg_hw_warm / avg_hw_cool
            print(f"  Ratio (warm/cool): {ratio:.2f}x")
    
    # -------------------------------------------------------------------------
    # Compile results
    # -------------------------------------------------------------------------
    results = {
        "correlation_days": corr_days,
        "correlation_intensity": corr_intensity,
        "correlation_extreme_days": corr_extreme,
        "avg_hw_warm_spring": avg_hw_warm,
        "avg_hw_cool_spring": avg_hw_cool,
        "avg_extreme_warm_spring": avg_extreme_warm,
        "avg_extreme_cool_spring": avg_extreme_cool,
        "warm_spring_cases": warm_count,
        "cool_spring_cases": cool_count,
        "sample_size": combined.count(),
        "heat_threshold_percentile": heat_threshold_percentile
    }
    
    if verbose:
        print("\n" + "-"*40)
        interpretation = "SUPPORTED" if (corr_days and corr_days > 0.1) else "NOT SUPPORTED"
        print(f"Hypothesis interpretation: {interpretation}")
        if corr_days and corr_days > 0.1:
            print("  Positive correlation confirms spring temps predict summer heat waves")
        else:
            print("  Weak or no relationship between spring temps and summer heat waves")
    
    return combined, results


def get_heat_wave_trends(df):
    """
    Analyze trends in heat wave frequency over time.
    
    Args:
        df: Climate data DataFrame
        
    Returns:
        DataFrame with yearly heat wave statistics
    """
    tmax_df = df.filter(col("ELEMENT") == "TMAX")
    summer_temps = tmax_df.filter(col("month").isin([6, 7, 8]))
    heat_wave_days = detect_heat_waves(summer_temps)
    
    # Aggregate by year
    yearly_trends = heat_wave_days \
        .groupBy("year") \
        .agg(
            sum(when(col("is_heat_wave_day"), 1).otherwise(0)).alias("total_hw_days"),
            count(when(col("is_heat_wave_day"), 1)).alias("hw_day_count"),
            avg("VALUE").alias("avg_summer_temp"),
            max("VALUE").alias("max_summer_temp")
        ) \
        .orderBy("year")
    
    return yearly_trends