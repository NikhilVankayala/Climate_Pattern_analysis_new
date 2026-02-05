CLIMATE PATTERN ANALYSIS (NOAA GHCN-DAILY)
=======================================

OVERVIEW
--------
This project implements a scalable data pipeline to collect, clean, and prepare
large-scale historical U.S. climate data for statistical analysis of long-term
weather patterns. The dataset is derived from NOAA’s Global Historical Climatology
Network – Daily (GHCN-Daily) and spans 25 years (2000–2024).

The final output is a 1+ GB, quality-controlled dataset suitable for regional
climate trend analysis, extreme weather studies, and statistical inference.
No machine learning methods are used in this project.


DATA SOURCE
-----------
Source: NOAA Global Historical Climatology Network – Daily (GHCN-Daily)
Description: Daily weather observations from thousands of U.S. weather stations.
Raw yearly files are approximately 350 MB before filtering.


DATASET SCOPE
-------------
Time Period:        2000–2024
Stations Selected:  5,500 (geographically stratified)
Active Stations:    ~4,290
Total Records:      ~25 million
Final File Size:    ~1.05 GB
Geographic Coverage:
  - Northeast
  - Southeast
  - Midwest
  - Southwest
  - West
  - Alaska
  - Hawaii


VARIABLES RETAINED
------------------
The dataset includes the following daily climate variables:

  TMAX  - Daily maximum temperature
  TMIN  - Daily minimum temperature
  PRCP  - Precipitation
  SNOW  - Snowfall
  SNWD  - Snow depth
  AWND  - Average wind speed

All values are converted to standard physical units.


DATA FORMAT
-----------
The final dataset is stored in long format with four columns:

  STATION  - Weather station ID
  DATE     - Observation date (YYYYMMDD)
  ELEMENT  - Variable type (e.g., TMAX, PRCP)
  VALUE    - Measured value

This format avoids sparse matrices and scales efficiently for Spark-based and
time-series analysis.


PIPELINE DESIGN
---------------
The pipeline is divided into three phases:

PHASE 1: STATION SELECTION
- Download NOAA station metadata (fixed-width text format)
- Filter for U.S. stations only (IDs starting with "US")
- Assign stations to seven geographic regions
- Perform stratified sampling to select 5,500 stations proportionally by region
- Within each region, sort stations by elevation and select evenly spaced stations
  to ensure geographic and climatic diversity

PHASE 2: DATA DOWNLOAD AND FILTERING
- Download yearly compressed NOAA data files (~350 MB each)
- Stream files line-by-line using gzip to avoid memory overload
- Filter records to selected stations only (~50 MB per year)
- Delete raw compressed files after filtering to conserve disk space
- Throttle requests to avoid overloading NOAA servers

PHASE 3: CLEANING AND CONSOLIDATION
- Concatenate all yearly filtered files into one dataset
- Remove records with quality flags (unreliable measurements)
- Filter to the six target climate variables
- Convert values from NOAA’s integer format to real units
- Output a single clean CSV (~1.05 GB)


SCRIPTS
-------
All scripts are located in the scripts/ directory:

  download_noaa_data.py
    - Downloads yearly GHCN-Daily compressed files from NOAA

  select_stations.py
    - Parses fixed-width station metadata
    - Performs stratified regional and elevation-based sampling

  download_filtered_data.py
    - Streams yearly data
    - Filters records to selected stations
    - Writes reduced yearly CSVs

  combine_without_pivot.py
    - Combines yearly filtered files into one long-format dataset

  check_stations.py
    - Verifies station counts and geographic coverage

  diagnosis_data.py
    - Performs data validation checks (size, record counts, coverage, date range)


QUALITY ASSURANCE
-----------------
The pipeline includes multiple validation checks:

- Dataset size verification (>= 1 GB)
- Record count validation (20–30 million expected)
- Station count verification
- Element coverage checks (e.g., precipitation coverage > 80%)
- Temporal completeness (all years present)
- Geographic coverage across all regions


INTENDED USE
------------
The final dataset is intended for:

- Regional climate trend analysis
- Statistical testing of temperature and precipitation changes
- Seasonal and interannual variability studies
- Extreme weather frequency analysis
- Large-scale processing using Apache Spark

This project focuses on data engineering and statistical analysis only.
Machine learning models are intentionally excluded.


REQUIREMENTS
------------
Python dependencies are listed in requirements.txt.
Core tools include Python, pandas, numpy, and gzip-based streaming.
