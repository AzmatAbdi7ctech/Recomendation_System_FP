from pyspark.sql.functions import date_format
def collection_category_tag_transformation(df=None,column_list=None):
    # df=df.filter((df.status!=2 ) & (df.status_for_sale!=2))
    columns=['id','category_id','tag_id']
    df=df.select(columns)
    df=df.toPandas()
    return df