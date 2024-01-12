from  Code.utils.libraries import *
def session(spark_driver=None):
    try:
        spark = SparkSession.builder \
        .appName("PySpark MySQL Example") \
        .config("spark.driver.extraClassPath", spark_driver) \
        .getOrCreate()
        print('connection created')
        return spark
    except: 
        print('define spark path')