# Spark Assignment

This repository contains the code and data for analyzing NYC taxi trip data using Apache Spark on Google Cloud Platform (GCP).

## Table of Contents
1. [Setup Instructions](#setup-instructions)
2. [Project Structure](#project-structure)
3. [Data](#data)
4. [Running the Analysis Jobs](#running-the-analysis-jobs)
5. [Expected Output](#expected-output)
6. [Results](#results)
7. [License](#license)

## Setup Instructions

### Prerequisites

- Python 3.8 or higher
- Apache Spark
- Google Cloud Platform account
- Git

### Steps

1. **Clone the repository:**
   ```bash
   git clone https://github.com/azizlahiani1639/sparkassignement.git
   cd sparkassignement
   
2. **Install the required Python packages:**
```bash
pip install -r requirements.txt
```
3. **Configure GCP and set up a Spark cluster:**
Follow the instructions in the provided assignment document to set up your Spark cluster on GCP.

## Project Structure
```bash
project/
│
├── data/                        # Input data files
│   ├── yellow_tripdata_2021-01.parquet
│   ├── yellow_tripdata_2021-02.parquet
│   └── ...                      # Additional data files
│
├── output/                      # Output files from analysis
│   ├── avg_fare_amount.csv
│   ├── avg_trip_distance.csv
│   └── ...
│
├── src/                         # Source code for analysis jobs
│   ├── jobs/
│   │   ├── demand_prediction.py
│   │   ├── fare_analysis.py
│   │   ├── tip_analysis.py
│   │   ├── traffic_analysis.py
│   │   └── trip_analysis.py
│   └── utils/
│       ├── spark_session.py
│
├── log4j.properties             # Log4j configuration file
├── requirements.txt             # Python dependencies
└── spark-submit                 # Script to submit Spark jobs
```
## Data
The data used in this project consists of NYC taxi trip records for the year 2021, available in Parquet format.

## Running the Analysis Jobs
### General Command
Use the spark-submit script to run each analysis job. Make sure your GCP Spark cluster is properly configured and running.

### Example Commands
Demand Prediction:
```bash
./spark-submit src/jobs/demand_prediction.py
```
Fare Analysis:
```bash
./spark-submit src/jobs/fare_analysis.py
```
Tip Analysis:
```bash
./spark-submit src/jobs/tip_analysis.py
```
Traffic Analysis:
```bash
./spark-submit src/jobs/traffic_analysis.py
```
Trip Analysis:
```bash
./spark-submit src/jobs/trip_analysis.py
```

## Expected Output

The results of the analyses will be saved in the output/ directory. Each analysis script generates specific output files:


- demand_prediction.py -> output/demand_prediction.csv

- fare_analysis.py -> output/avg_fare_amount.csv

- tip_analysis.py -> output/tip_by_location.csv, output/tip_by_time.csv

- traffic_analysis.py -> output/trip_count.csv

- trip_analysis.py -> output/avg_trip_distance.csv

## Results
After running the jobs, you should find the following output files in the output/ directory:

- avg_fare_amount.csv: Average fare amount for each month.

- avg_trip_distance.csv: Average trip distance for each month.

- demand_prediction.csv: Predicted demand for taxis.

- tip_by_location.csv: Tips categorized by pickup location.

- tip_by_time.csv: Tips categorized by time of day.

- trip_count.csv: Number of trips per day.
