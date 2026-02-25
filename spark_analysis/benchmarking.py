import time
import argparse
import os
import csv
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rand

# Import hypothesis functions
from hypothesis1 import analyze_winter_precipitation_lag
from hypothesis2 import analyze_spring_summer_heat
from hypothesis3 import analyze_wind_temperature_moderation
from hypothesis4 import analyze_snowmelt_spring_precipitation


# =============================================================================
# SPARK SESSION WITH CONFIGURABLE WORKERS
# =============================================================================

def create_spark_session(num_workers=None, app_name="ClimateAnalysisBenchmark"):
    """
    Create Spark session with specified number of workers.
    
    Args:
        num_workers: Number of cores/workers (None = all available, or specify 1,2,4,8)
        app_name: Application name
        
    Returns:
        SparkSession
    """
    master = f"local[{num_workers}]" if num_workers else "local[*]"
    
    spark = SparkSession.builder \
        .appName(f"{app_name}_workers{num_workers}") \
        .master(master) \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "200") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark


# =============================================================================
# DATA LOADING WITH SIZE CONTROL
# =============================================================================

def load_climate_data(spark, filepath, sample_fraction=1.0):
    """
    Load climate data with optional sampling.
    
    Args:
        spark: SparkSession
        filepath: Path to CSV
        sample_fraction: Fraction of data to use (0.25, 0.5, 0.75, 1.0)
        
    Returns:
        DataFrame
    """
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
    
    schema = StructType([
        StructField("STATION", StringType(), False),
        StructField("DATE", IntegerType(), False),
        StructField("ELEMENT", StringType(), False),
        StructField("VALUE", DoubleType(), True)
    ])
    
    df = spark.read.csv(filepath, schema=schema, header=True)
    
    # Sample if needed
    if sample_fraction < 1.0:
        df = df.sample(fraction=sample_fraction, seed=42)
    
    # Add derived columns
    df = df.withColumn("year", (col("DATE") / 10000).cast("int")) \
           .withColumn("month", ((col("DATE") % 10000) / 100).cast("int")) \
           .withColumn("day", (col("DATE") % 100).cast("int"))
    
    return df


# =============================================================================
# BENCHMARK RUNNER
# =============================================================================

def run_single_benchmark(spark, df, hypothesis_num):
    """
    Run a single hypothesis analysis and measure time.
    
    Args:
        spark: SparkSession
        df: Climate DataFrame
        hypothesis_num: Which hypothesis (1-4)
        
    Returns:
        float: Execution time in seconds
    """
    start_time = time.time()
    
    try:
        if hypothesis_num == 1:
            _, _ = analyze_winter_precipitation_lag(df, verbose=False)
        elif hypothesis_num == 2:
            _, _ = analyze_spring_summer_heat(df, verbose=False)
        elif hypothesis_num == 3:
            _, _ = analyze_wind_temperature_moderation(df, verbose=False)
        elif hypothesis_num == 4:
            _, _ = analyze_snowmelt_spring_precipitation(df, verbose=False)
    except Exception as e:
        print(f"  Error in H{hypothesis_num}: {e}")
        return None
    
    end_time = time.time()
    return end_time - start_time


def run_all_hypotheses_benchmark(spark, df):
    """
    Run all hypotheses and measure total time.
    
    Returns:
        dict: Times for each hypothesis and total
    """
    times = {}
    total_start = time.time()
    
    for h in [1, 2, 3, 4]:
        t = run_single_benchmark(spark, df, h)
        times[f"H{h}"] = t
        print(f"    H{h}: {t:.2f}s" if t else f"    H{h}: Error")
    
    times["total"] = time.time() - total_start
    return times


# =============================================================================
# BENCHMARK EXPERIMENTS
# =============================================================================

def benchmark_varying_workers(data_path, output_dir, worker_counts=[1, 2, 4, 8]):
    """
    Benchmark with varying number of Spark workers.
    
    Args:
        data_path: Path to climate data
        output_dir: Output directory for results
        worker_counts: List of worker counts to test
        
    Returns:
        list: Benchmark results
    """
    print("\n" + "="*60)
    print("BENCHMARK: Varying Number of Workers")
    print("="*60)
    
    results = []
    
    for num_workers in worker_counts:
        print(f"\n--- Testing with {num_workers} worker(s) ---")
        
        # Create new Spark session with specified workers
        spark = create_spark_session(num_workers)
        
        # Load full dataset
        df = load_climate_data(spark, data_path, sample_fraction=1.0)
        record_count = df.count()
        print(f"  Records: {record_count:,}")
        
        # Run benchmark
        times = run_all_hypotheses_benchmark(spark, df)
        
        results.append({
            "experiment": "workers",
            "workers": num_workers,
            "data_fraction": 1.0,
            "records": record_count,
            "H1_time": times.get("H1"),
            "H2_time": times.get("H2"),
            "H3_time": times.get("H3"),
            "H4_time": times.get("H4"),
            "total_time": times.get("total")
        })
        
        print(f"  Total time: {times['total']:.2f}s")
        
        spark.stop()
    
    return results


def benchmark_varying_data_size(data_path, output_dir, fractions=[0.25, 0.5, 0.75, 1.0], num_workers=4):
    """
    Benchmark with varying data sizes.
    
    Args:
        data_path: Path to climate data
        output_dir: Output directory for results
        fractions: List of data fractions to test
        num_workers: Number of workers to use
        
    Returns:
        list: Benchmark results
    """
    print("\n" + "="*60)
    print(f"BENCHMARK: Varying Data Size (using {num_workers} workers)")
    print("="*60)
    
    results = []
    
    # Create Spark session once
    spark = create_spark_session(num_workers)
    
    for fraction in fractions:
        print(f"\n--- Testing with {int(fraction*100)}% of data ---")
        
        # Load sampled dataset
        df = load_climate_data(spark, data_path, sample_fraction=fraction)
        record_count = df.count()
        print(f"  Records: {record_count:,}")
        
        # Run benchmark
        times = run_all_hypotheses_benchmark(spark, df)
        
        results.append({
            "experiment": "datasize",
            "workers": num_workers,
            "data_fraction": fraction,
            "records": record_count,
            "H1_time": times.get("H1"),
            "H2_time": times.get("H2"),
            "H3_time": times.get("H3"),
            "H4_time": times.get("H4"),
            "total_time": times.get("total")
        })
        
        print(f"  Total time: {times['total']:.2f}s")
    
    spark.stop()
    return results


# =============================================================================
# SAVE RESULTS
# =============================================================================

def save_results_csv(results, output_path):
    """Save benchmark results to CSV."""
    fieldnames = [
        "experiment", "workers", "data_fraction", "records",
        "H1_time", "H2_time", "H3_time", "H4_time", "total_time"
    ]
    
    with open(output_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)
    
    print(f"\nResults saved to {output_path}")


# =============================================================================
# GENERATE GRAPHS
# =============================================================================

def generate_graphs(results, output_dir):
    """
    Generate benchmark visualization graphs.
    
    Args:
        results: List of benchmark result dicts
        output_dir: Output directory for PNG files
    """
    try:
        import matplotlib.pyplot as plt
        import numpy as np
    except ImportError:
        print("\nWARNING: matplotlib not installed. Run: pip install matplotlib")
        print("Skipping graph generation. Use the CSV file to create graphs manually.")
        return
    
    # Separate results by experiment type
    worker_results = [r for r in results if r["experiment"] == "workers"]
    datasize_results = [r for r in results if r["experiment"] == "datasize"]
    
    # Set style
    plt.style.use('seaborn-v0_8-whitegrid')
    
    # -------------------------------------------------------------------------
    # Graph 1: Execution Time vs Number of Workers
    # -------------------------------------------------------------------------
    if worker_results:
        fig, ax = plt.subplots(figsize=(10, 6))
        
        workers = [r["workers"] for r in worker_results]
        total_times = [r["total_time"] for r in worker_results]
        h1_times = [r["H1_time"] or 0 for r in worker_results]
        h2_times = [r["H2_time"] or 0 for r in worker_results]
        h3_times = [r["H3_time"] or 0 for r in worker_results]
        h4_times = [r["H4_time"] or 0 for r in worker_results]
        
        x = np.arange(len(workers))
        width = 0.15
        
        ax.bar(x - 2*width, h1_times, width, label='H1: Winter Precip', color='#2ecc71')
        ax.bar(x - width, h2_times, width, label='H2: Heat Waves', color='#e74c3c')
        ax.bar(x, h3_times, width, label='H3: Wind Moderation', color='#3498db')
        ax.bar(x + width, h4_times, width, label='H4: Snowmelt', color='#9b59b6')
        ax.bar(x + 2*width, total_times, width, label='Total', color='#34495e')
        
        ax.set_xlabel('Number of Spark Workers', fontsize=12)
        ax.set_ylabel('Execution Time (seconds)', fontsize=12)
        ax.set_title('Execution Time vs Number of Workers', fontsize=14, fontweight='bold')
        ax.set_xticks(x)
        ax.set_xticklabels(workers)
        ax.legend()
        
        # Add value labels
        for i, v in enumerate(total_times):
            ax.text(i + 2*width, v + 0.5, f'{v:.1f}s', ha='center', fontsize=9)
        
        plt.tight_layout()
        plt.savefig(f"{output_dir}/workers_benchmark.png", dpi=150)
        plt.close()
        print(f"Saved: {output_dir}/workers_benchmark.png")
    
    # -------------------------------------------------------------------------
    # Graph 2: Execution Time vs Data Size
    # -------------------------------------------------------------------------
    if datasize_results:
        fig, ax = plt.subplots(figsize=(10, 6))
        
        fractions = [f"{int(r['data_fraction']*100)}%" for r in datasize_results]
        records = [r["records"] for r in datasize_results]
        total_times = [r["total_time"] for r in datasize_results]
        h1_times = [r["H1_time"] or 0 for r in datasize_results]
        h2_times = [r["H2_time"] or 0 for r in datasize_results]
        h3_times = [r["H3_time"] or 0 for r in datasize_results]
        h4_times = [r["H4_time"] or 0 for r in datasize_results]
        
        x = np.arange(len(fractions))
        width = 0.15
        
        ax.bar(x - 2*width, h1_times, width, label='H1: Winter Precip', color='#2ecc71')
        ax.bar(x - width, h2_times, width, label='H2: Heat Waves', color='#e74c3c')
        ax.bar(x, h3_times, width, label='H3: Wind Moderation', color='#3498db')
        ax.bar(x + width, h4_times, width, label='H4: Snowmelt', color='#9b59b6')
        ax.bar(x + 2*width, total_times, width, label='Total', color='#34495e')
        
        ax.set_xlabel('Data Size (% of full dataset)', fontsize=12)
        ax.set_ylabel('Execution Time (seconds)', fontsize=12)
        ax.set_title('Execution Time vs Data Size', fontsize=14, fontweight='bold')
        ax.set_xticks(x)
        ax.set_xticklabels([f"{f}\n({r/1e6:.1f}M)" for f, r in zip(fractions, records)])
        ax.legend()
        
        # Add value labels
        for i, v in enumerate(total_times):
            ax.text(i + 2*width, v + 0.5, f'{v:.1f}s', ha='center', fontsize=9)
        
        plt.tight_layout()
        plt.savefig(f"{output_dir}/datasize_benchmark.png", dpi=150)
        plt.close()
        print(f"Saved: {output_dir}/datasize_benchmark.png")
    
    # -------------------------------------------------------------------------
    # Graph 3: Line Plot - Speedup Analysis
    # -------------------------------------------------------------------------
    if worker_results:
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))
        
        workers = [r["workers"] for r in worker_results]
        total_times = [r["total_time"] for r in worker_results]
        
        # Left: Execution time
        ax1.plot(workers, total_times, 'bo-', linewidth=2, markersize=10)
        ax1.set_xlabel('Number of Workers', fontsize=12)
        ax1.set_ylabel('Execution Time (seconds)', fontsize=12)
        ax1.set_title('Execution Time vs Workers', fontsize=14, fontweight='bold')
        ax1.set_xticks(workers)
        ax1.grid(True, alpha=0.3)
        
        # Add value labels
        for x, y in zip(workers, total_times):
            ax1.annotate(f'{y:.1f}s', (x, y), textcoords="offset points", 
                        xytext=(0,10), ha='center')
        
        # Right: Speedup
        baseline = total_times[0]  # 1 worker as baseline
        speedups = [baseline / t for t in total_times]
        ideal_speedup = workers  # Ideal linear speedup
        
        ax2.plot(workers, speedups, 'go-', linewidth=2, markersize=10, label='Actual Speedup')
        ax2.plot(workers, ideal_speedup, 'r--', linewidth=2, label='Ideal Linear Speedup')
        ax2.set_xlabel('Number of Workers', fontsize=12)
        ax2.set_ylabel('Speedup (relative to 1 worker)', fontsize=12)
        ax2.set_title('Speedup Analysis', fontsize=14, fontweight='bold')
        ax2.set_xticks(workers)
        ax2.legend()
        ax2.grid(True, alpha=0.3)
        
        # Add value labels
        for x, y in zip(workers, speedups):
            ax2.annotate(f'{y:.2f}x', (x, y), textcoords="offset points", 
                        xytext=(0,10), ha='center')
        
        plt.tight_layout()
        plt.savefig(f"{output_dir}/speedup_analysis.png", dpi=150)
        plt.close()
        print(f"Saved: {output_dir}/speedup_analysis.png")
    
    # -------------------------------------------------------------------------
    # Graph 4: Scalability Analysis (Data Size)
    # -------------------------------------------------------------------------
    if datasize_results:
        fig, ax = plt.subplots(figsize=(10, 6))
        
        records = [r["records"] / 1e6 for r in datasize_results]  # In millions
        total_times = [r["total_time"] for r in datasize_results]
        
        ax.plot(records, total_times, 'bo-', linewidth=2, markersize=10)
        
        # Fit linear trend
        z = np.polyfit(records, total_times, 1)
        p = np.poly1d(z)
        ax.plot(records, p(records), 'r--', linewidth=1.5, label=f'Linear fit (slope={z[0]:.2f}s/M records)')
        
        ax.set_xlabel('Data Size (millions of records)', fontsize=12)
        ax.set_ylabel('Execution Time (seconds)', fontsize=12)
        ax.set_title('Scalability: Execution Time vs Data Size', fontsize=14, fontweight='bold')
        ax.legend()
        ax.grid(True, alpha=0.3)
        
        # Add value labels
        for x, y in zip(records, total_times):
            ax.annotate(f'{y:.1f}s', (x, y), textcoords="offset points", 
                        xytext=(0,10), ha='center')
        
        plt.tight_layout()
        plt.savefig(f"{output_dir}/scalability_analysis.png", dpi=150)
        plt.close()
        print(f"Saved: {output_dir}/scalability_analysis.png")
    
    # -------------------------------------------------------------------------
    # Graph 5: Hypothesis Comparison (Pie Chart)
    # -------------------------------------------------------------------------
    if worker_results:
        # Use 4-worker results for comparison
        r = next((r for r in worker_results if r["workers"] == 4), worker_results[-1])
        
        fig, ax = plt.subplots(figsize=(8, 8))
        
        labels = ['H1: Winter Precip', 'H2: Heat Waves', 'H3: Wind Mod', 'H4: Snowmelt']
        times = [r["H1_time"] or 0, r["H2_time"] or 0, r["H3_time"] or 0, r["H4_time"] or 0]
        colors = ['#2ecc71', '#e74c3c', '#3498db', '#9b59b6']
        
        # Filter out zeros
        non_zero = [(l, t, c) for l, t, c in zip(labels, times, colors) if t > 0]
        if non_zero:
            labels, times, colors = zip(*non_zero)
            
            ax.pie(times, labels=labels, colors=colors, autopct='%1.1f%%',
                   startangle=90, explode=[0.02]*len(times))
            ax.set_title('Execution Time Distribution by Hypothesis\n(4 workers, full data)', 
                        fontsize=14, fontweight='bold')
            
            plt.tight_layout()
            plt.savefig(f"{output_dir}/hypothesis_distribution.png", dpi=150)
            plt.close()
            print(f"Saved: {output_dir}/hypothesis_distribution.png")


# =============================================================================
# MAIN
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description="Benchmark Climate Analysis")
    parser.add_argument("--data", type=str, required=True, help="Path to climate data CSV")
    parser.add_argument("--output", type=str, default="results", help="Output directory")
    parser.add_argument("--workers", type=str, default="1,2,4,8", 
                        help="Comma-separated worker counts (default: 1,2,4,8)")
    parser.add_argument("--fractions", type=str, default="0.25,0.5,0.75,1.0",
                        help="Comma-separated data fractions (default: 0.25,0.5,0.75,1.0)")
    parser.add_argument("--skip-workers", action="store_true", help="Skip worker benchmark")
    parser.add_argument("--skip-datasize", action="store_true", help="Skip data size benchmark")
    
    args = parser.parse_args()
    
    # Parse arguments
    worker_counts = [int(w) for w in args.workers.split(",")]
    fractions = [float(f) for f in args.fractions.split(",")]
    
    # Create output directory
    os.makedirs(args.output, exist_ok=True)
    
    print("\n" + "="*60)
    print("CLIMATE ANALYSIS BENCHMARKING")
    print("="*60)
    print(f"Data: {args.data}")
    print(f"Output: {args.output}")
    print(f"Worker counts: {worker_counts}")
    print(f"Data fractions: {fractions}")
    
    all_results = []
    
    # Run benchmarks
    if not args.skip_workers:
        worker_results = benchmark_varying_workers(args.data, args.output, worker_counts)
        all_results.extend(worker_results)
    
    if not args.skip_datasize:
        datasize_results = benchmark_varying_data_size(args.data, args.output, fractions)
        all_results.extend(datasize_results)
    
    # Save results
    save_results_csv(all_results, f"{args.output}/benchmark_results.csv")
    
    # Generate graphs
    generate_graphs(all_results, args.output)
    
    print("\n" + "="*60)
    print("BENCHMARKING COMPLETE")
    print("="*60)
    print(f"\nOutput files in {args.output}/:")
    print("  - benchmark_results.csv")
    print("  - workers_benchmark.png")
    print("  - datasize_benchmark.png")
    print("  - speedup_analysis.png")
    print("  - scalability_analysis.png")
    print("  - hypothesis_distribution.png")


if __name__ == "__main__":
    main()