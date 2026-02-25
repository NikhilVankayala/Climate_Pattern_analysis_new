import matplotlib.pyplot as plt
import numpy as np

# Benchmark data from server
workers_data = {
    "workers": [1, 2, 4, 8],
    "H1_time": [469.32, 255.85, 142.25, 141.44],
    "H2_time": [945.18, 530.11, 300.95, 305.70],
    "H3_time": [673.60, 366.43, 208.24, 214.78],
    "H4_time": [1789.67, 960.43, 540.30, 545.82],
    "total_time": [3877.76, 2112.83, 1191.74, 1207.75]
}

datasize_data = {
    "fraction": ["25%", "50%", "75%", "100%"],
    "records": [8.69, 17.38, 26.06, 34.75],  # millions
    "H1_time": [152.30, 153.76, 160.27, 145.70],
    "H2_time": [323.30, 324.60, 370.00, 312.97],
    "H3_time": [183.23, 199.83, 219.11, 212.11],
    "H4_time": [72.22, 72.22, 652.96, 536.17],
    "total_time": [731.04, 750.42, 1402.33, 1206.95]
}

# Set style
plt.style.use('seaborn-v0_8-whitegrid')

# Graph 1: Execution Time vs Number of Workers (Bar Chart)
fig, ax = plt.subplots(figsize=(12, 7))

x = np.arange(len(workers_data["workers"]))
width = 0.15

ax.bar(x - 2*width, workers_data["H1_time"], width, label='H1: Winter Precip', color='#2ecc71')
ax.bar(x - width, workers_data["H2_time"], width, label='H2: Heat Waves', color='#e74c3c')
ax.bar(x, workers_data["H3_time"], width, label='H3: Wind Moderation', color='#3498db')
ax.bar(x + width, workers_data["H4_time"], width, label='H4: Snowmelt', color='#9b59b6')
ax.bar(x + 2*width, workers_data["total_time"], width, label='Total', color='#34495e')

ax.set_xlabel('Number of Spark Workers', fontsize=14)
ax.set_ylabel('Execution Time (seconds)', fontsize=14)
ax.set_title('Execution Time vs Number of Workers', fontsize=16, fontweight='bold')
ax.set_xticks(x)
ax.set_xticklabels(workers_data["workers"])
ax.legend(loc='upper right')

# Add value labels on total bars
for i, v in enumerate(workers_data["total_time"]):
    ax.text(i + 2*width, v + 50, f'{v:.0f}s', ha='center', fontsize=10)

plt.tight_layout()
plt.savefig('workers_benchmark.png', dpi=150)
plt.close()
print("Saved: workers_benchmark.png")

# Graph 2: Execution Time vs Data Size (Bar Chart)
fig, ax = plt.subplots(figsize=(12, 7))

x = np.arange(len(datasize_data["fraction"]))
width = 0.15

ax.bar(x - 2*width, datasize_data["H1_time"], width, label='H1: Winter Precip', color='#2ecc71')
ax.bar(x - width, datasize_data["H2_time"], width, label='H2: Heat Waves', color='#e74c3c')
ax.bar(x, datasize_data["H3_time"], width, label='H3: Wind Moderation', color='#3498db')
ax.bar(x + width, datasize_data["H4_time"], width, label='H4: Snowmelt', color='#9b59b6')
ax.bar(x + 2*width, datasize_data["total_time"], width, label='Total', color='#34495e')

ax.set_xlabel('Data Size (% of full dataset)', fontsize=14)
ax.set_ylabel('Execution Time (seconds)', fontsize=14)
ax.set_title('Execution Time vs Data Size (4 Workers)', fontsize=16, fontweight='bold')
ax.set_xticks(x)
ax.set_xticklabels([f"{f}\n({r:.1f}M)" for f, r in zip(datasize_data["fraction"], datasize_data["records"])])
ax.legend(loc='upper left')

# Add value labels on total bars
for i, v in enumerate(datasize_data["total_time"]):
    ax.text(i + 2*width, v + 30, f'{v:.0f}s', ha='center', fontsize=10)

plt.tight_layout()
plt.savefig('datasize_benchmark.png', dpi=150)
plt.close()
print("Saved: datasize_benchmark.png")

# Graph 3: Speedup Analysis (Line Plot)
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))

workers = workers_data["workers"]
total_times = workers_data["total_time"]

# Left: Execution time
ax1.plot(workers, total_times, 'bo-', linewidth=2, markersize=10)
ax1.set_xlabel('Number of Workers', fontsize=14)
ax1.set_ylabel('Execution Time (seconds)', fontsize=14)
ax1.set_title('Execution Time vs Workers', fontsize=14, fontweight='bold')
ax1.set_xticks(workers)
ax1.grid(True, alpha=0.3)

for x, y in zip(workers, total_times):
    ax1.annotate(f'{y:.0f}s', (x, y), textcoords="offset points", xytext=(0, 15), ha='center', fontsize=11)

# Right: Speedup
baseline = total_times[0]
speedups = [baseline / t for t in total_times]
ideal_speedup = workers

ax2.plot(workers, speedups, 'go-', linewidth=2, markersize=10, label='Actual Speedup')
ax2.plot(workers, ideal_speedup, 'r--', linewidth=2, label='Ideal Linear Speedup')
ax2.set_xlabel('Number of Workers', fontsize=14)
ax2.set_ylabel('Speedup (relative to 1 worker)', fontsize=14)
ax2.set_title('Speedup Analysis', fontsize=14, fontweight='bold')
ax2.set_xticks(workers)
ax2.legend(fontsize=12)
ax2.grid(True, alpha=0.3)

for x, y in zip(workers, speedups):
    ax2.annotate(f'{y:.2f}x', (x, y), textcoords="offset points", xytext=(0, 15), ha='center', fontsize=11)

plt.tight_layout()
plt.savefig('speedup_analysis.png', dpi=150)
plt.close()
print("Saved: speedup_analysis.png")

# Graph 4: Scalability Analysis (Line Plot)
fig, ax = plt.subplots(figsize=(10, 7))

records = datasize_data["records"]
total_times = datasize_data["total_time"]

ax.plot(records, total_times, 'bo-', linewidth=2, markersize=12)

# Fit linear trend (excluding outliers if needed)
z = np.polyfit(records, total_times, 1)
p = np.poly1d(z)
x_line = np.linspace(min(records), max(records), 100)
ax.plot(x_line, p(x_line), 'r--', linewidth=2, label=f'Linear fit (slope={z[0]:.1f}s per M records)')

ax.set_xlabel('Data Size (millions of records)', fontsize=14)
ax.set_ylabel('Execution Time (seconds)', fontsize=14)
ax.set_title('Scalability: Execution Time vs Data Size', fontsize=16, fontweight='bold')
ax.legend(fontsize=12)
ax.grid(True, alpha=0.3)

for x, y in zip(records, total_times):
    ax.annotate(f'{y:.0f}s', (x, y), textcoords="offset points", xytext=(0, 15), ha='center', fontsize=11)

plt.tight_layout()
plt.savefig('scalability_analysis.png', dpi=150)
plt.close()
print("Saved: scalability_analysis.png")

# Graph 5: Hypothesis Time Distribution (Pie Chart)
fig, ax = plt.subplots(figsize=(10, 10))

# Use 4-worker data
labels = ['H1: Winter Precip', 'H2: Heat Waves', 'H3: Wind Moderation', 'H4: Snowmelt']
times = [142.25, 300.95, 208.24, 540.30]
colors = ['#2ecc71', '#e74c3c', '#3498db', '#9b59b6']

ax.pie(times, labels=labels, colors=colors, autopct='%1.1f%%', startangle=90, 
       explode=[0.02, 0.02, 0.02, 0.02], textprops={'fontsize': 12})
ax.set_title('Execution Time Distribution by Hypothesis\n(4 workers, full data)', 
             fontsize=16, fontweight='bold')

plt.tight_layout()
plt.savefig('hypothesis_distribution.png', dpi=150)
plt.close()
print("Saved: hypothesis_distribution.png")

print("\n✓ All graphs generated successfully!")
print("\nFiles created:")
print("  - workers_benchmark.png")
print("  - datasize_benchmark.png")
print("  - speedup_analysis.png")
print("  - scalability_analysis.png")
print("  - hypothesis_distribution.png")