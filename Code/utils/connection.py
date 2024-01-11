from utils.lib import *

def Session(path_driver=None):
    spark = SparkSession.builder \
        .appName("PySpark MySQL Example") \
        .config("spark.driver.extraClassPath", "C:/spark/spark-3.5.0-bin-hadoop3/jars/mysql-connector-java-5.1.46.jar") \
        .getOrCreate()
    return spark

spark=Session()