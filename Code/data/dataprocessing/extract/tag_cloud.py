from pyspark.sql.functions import date_format
def tag_cloud_transformation(df=None,column_list=None):
    # df=df.filter((df.status!=2 ) & (df.status_for_sale!=2))
    columns=['id','tag_name']
    df=df.select(columns)
    df=df.toPandas()
    return df