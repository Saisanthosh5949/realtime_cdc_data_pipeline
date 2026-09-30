from pyspark.sql import SparkSession
def get_spark_session(app_name):
    spark=(SparkSession.builder.appName(app_name)
           .config("spark.sql.session.timeZone","UTC")
           .config("spark.sql.shuffle.partitions","8")
           .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    return spark
