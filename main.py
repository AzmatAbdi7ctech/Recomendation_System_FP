from Code.utils.libraries import * 
from Code.data.dataprocessing.extract.extract import *


def session():
    spark = SparkSession.builder \
    .appName("PySpark MySQL Example") \
    .config("spark.driver.extraClassPath", "C:/spark/spark-3.5.0-bin-hadoop3/jars/mysql-connector-java-5.1.46.jar") \
    .getOrCreate()
    return spark
spark_session=session()

fetch_size=1000000
df=extract_data(spark=spark_session,table_name=table_name,password=password,user_name=user_name,fetech_size=fetch_size,conn_url=conn_url)
df.show()
sys.exit()




