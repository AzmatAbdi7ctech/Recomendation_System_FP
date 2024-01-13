from pyspark.sql.functions import date_format
def product_transformation(df=None,column_list=None):
    df=df.filter((df.status!=2 ) & (df.status_for_sale!=2))
    columns=['product_id','product_title','published_at','product_thumbnail','created_at','updated_at']
    df=df.select(columns)
    df=df.withColumn("published_at", date_format("published_at", "yyyy-MM-dd HH:mm:ss"))
    df=df.withColumn("created_at", date_format("created_at", "yyyy-MM-dd HH:mm:ss"))
    df=df.withColumn("updated_at", date_format("updated_at", "yyyy-MM-dd HH:mm:ss"))
    df=df.toPandas()
    return df
