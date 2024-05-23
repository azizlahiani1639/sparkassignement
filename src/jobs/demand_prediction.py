import os
import glob
from pyspark.sql import SparkSession
from pyspark.sql.functions import hour, dayofweek, month, dayofmonth, col
from pyspark.sql.functions import unix_timestamp, window
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator

def get_spark_session(app_name):
    return SparkSession.builder.appName(app_name).getOrCreate()

def main():
    try:
        spark = get_spark_session('DemandPrediction')
        
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
    
        # Feature engineering
        df = df.withColumn('hour', hour(df['tpep_pickup_datetime']))
        df = df.withColumn('day_of_week', dayofweek(df['tpep_pickup_datetime']))
        df = df.withColumn('month', month(df['tpep_pickup_datetime']))
        df = df.withColumn('day_of_month', dayofmonth(df['tpep_pickup_datetime']))
        df = df.withColumn('timestamp', unix_timestamp(df['tpep_pickup_datetime']))

        # Create a window for hourly aggregation
        hourly_pickups = df.groupBy(window(df['tpep_pickup_datetime'], "1 hour")).count()
        hourly_pickups = hourly_pickups.withColumnRenamed('count', 'pickups')
        hourly_pickups.show()

        # Join the hourly pickups back to the original dataframe
        df = df.join(hourly_pickups, df['tpep_pickup_datetime'].between(hourly_pickups['window.start'], hourly_pickups['window.end']), 'inner')
        df.show(5)

        # Prepare the data for the regression model
        feature_cols = ['hour', 'day_of_week', 'month', 'day_of_month']
        assembler = VectorAssembler(inputCols=feature_cols, outputCol='features')
        df = assembler.transform(df)

        # Split data into training and test sets
        train_data, test_data = df.randomSplit([0.8, 0.2], seed=42)

        # Train a linear regression model
        lr = LinearRegression(featuresCol='features', labelCol='pickups')
        lr_model = lr.fit(train_data)

        # Make predictions
        predictions = lr_model.transform(test_data)
        predictions.show()

        # Evaluate the model
        evaluator = RegressionEvaluator(predictionCol='prediction', labelCol='pickups', metricName='rmse')
        rmse = evaluator.evaluate(predictions)
        print(f"Root Mean Squared Error (RMSE) on test data: {rmse}")

        # Convert predictions to Pandas DataFrame and save to CSV
        pandas_df = predictions.select('window', 'prediction').toPandas()

        # Define the output path in the output folder
        output_folder = "output"
        final_output_path = os.path.join(output_folder, "demand_prediction.csv")
        
        # Ensure the output directory exists
        os.makedirs(output_folder, exist_ok=True)

        # Save the Pandas DataFrame to a CSV file
        pandas_df.to_csv(final_output_path, index=False)
        
        print(f"Results saved to {final_output_path}")
        
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
