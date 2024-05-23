import os
import glob
import pandas as pd
from pyspark.sql.functions import avg, col, hour, dayofweek, month
from utils.spark_session import get_spark_session

def main():
    try:
        spark = get_spark_session('TipAnalysis')
        
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
    
        # Calculate tip percentage
        df = df.withColumn("tip_percentage", (col("tip_amount") / col("fare_amount")) * 100)
    
        # Tip percentage by pickup location
        tip_by_location = df.groupBy("PULocationID").agg(avg("tip_percentage").alias("avg_tip_percentage"))
    
        # Tip percentage by time
        df = df.withColumn("hour", hour(col("tpep_pickup_datetime")))
        df = df.withColumn("day_of_week", dayofweek(col("tpep_pickup_datetime")))
        df = df.withColumn("month", month(col("tpep_pickup_datetime")))
    
        tip_by_time = df.groupBy("hour", "day_of_week", "month").agg(avg("tip_percentage").alias("avg_tip_percentage"))
        
        # Convert Spark DataFrames to Pandas DataFrames
        tip_by_location_pd = tip_by_location.toPandas()
        tip_by_time_pd = tip_by_time.toPandas()
        
        # Define the output path in the output folder
        output_folder = "output"
        os.makedirs(output_folder, exist_ok=True)
        
        # Save the Pandas DataFrames to CSV files
        tip_by_location_output_path = os.path.join(output_folder, "tip_by_location.csv")
        tip_by_time_output_path = os.path.join(output_folder, "tip_by_time.csv")
        
        tip_by_location_pd.to_csv(tip_by_location_output_path, index=False)
        tip_by_time_pd.to_csv(tip_by_time_output_path, index=False)
        
        print(f"Tip by location results saved to {tip_by_location_output_path}")
        print(f"Tip by time results saved to {tip_by_time_output_path}")
        
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
