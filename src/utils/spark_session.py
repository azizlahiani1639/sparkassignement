from pyspark.sql import SparkSession

def get_spark_session(app_name: str):
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
        .getOrCreate()
