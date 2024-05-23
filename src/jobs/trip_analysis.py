import os
import glob
import pandas as pd
from pyspark.sql.functions import avg, hour, dayofweek, month
from utils.spark_session import get_spark_session

def main():
    try:
        spark = get_spark_session('TripAnalysis')
        
        # Use glob to manually expand the file paths
        input_path_pattern = 'data/yellow_tripdata_*.parquet'
        print(f"Expanding parquet files from: {input_path_pattern}")
        
        input_files = glob.glob(input_path_pattern)
        if not input_files:
            print(f"No files matched the pattern: {input_path_pattern}")
            return
        
        print(f"Reading parquet files: {input_files}")
        
        # Load data
        df = spark.read.parquet(*input_files)
        print("Successfully read parquet files.")
        df.show(5)
    
        # Average duration and distance by time of day, day of week, and month of year
        df = df.withColumn('hour', hour(df['tpep_pickup_datetime']))
        df = df.withColumn('day_of_week', dayofweek(df['tpep_pickup_datetime']))
        df = df.withColumn('month', month(df['tpep_pickup_datetime']))
    
        avg_duration_distance = df.groupBy('hour', 'day_of_week', 'month').agg(
            avg('trip_distance').alias('avg_distance')
        )
        
        avg_duration_distance.show()

        # Convert Spark DataFrame to Pandas DataFrame
        pandas_df = avg_duration_distance.toPandas()

        # Define the output path in the output folder
        output_folder = "output"
        final_output_path = os.path.join(output_folder, "avg_trip_distance.csv")
        
        # Ensure the output directory exists
        os.makedirs(output_folder, exist_ok=True)

        # Save the Pandas DataFrame to a CSV file
        pandas_df.to_csv(final_output_path, index=False)
        
        print(f"Results saved to {final_output_path}")
        
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
