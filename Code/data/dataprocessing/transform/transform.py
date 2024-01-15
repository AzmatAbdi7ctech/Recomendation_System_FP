from pyspark.sql.functions import concat,concat_ws
def feature_list(df):
      
  category_collection_FE=[i[0] for i in df.select('category_collection_FE').collect()]
  category_collection_tag_L=[i[0] for i in df.select('category_collection_tag_L').collect()]
  tag_cloud=[i[0] for i in df.select('tag_cloud').collect()]

  return {'category_collection_FE':category_collection_FE,'categoey_collection_tag_L':category_collection_tag_L,'tag_cloud':tag_cloud}

def string_column(data_df):
    for key,value in data_df.items():
        data_df[key]=[key if (v != '' and int(v) >0) else '' for v in value]
        # print([ key if  v != '0' else '' for v in value ])
        # print([key if (v != '' and int(v) >0) else '' for v in value])
    return data_df

def tag_cloud_join_df(collection_category_df,collection_category_tag_df,tag_cloud_df,product_tag_df,feature_list,join_type):
  product_product_tag_df=product_tag_df.join(tag_cloud_df,product_tag_df.tag_id==tag_cloud_df.id,'left')
  product_product_tag_df=product_product_tag_df.filter(product_product_tag_df['tag_id'].isin(feature_list['tag_cloud']))
  collection_product_product_tag_df=product_product_tag_df.join(collection_category_tag_df,collection_category_tag_df.tag_id==product_product_tag_df.tag_id,'left')
  collection_product_product_tag_df=collection_product_product_tag_df.join(collection_category_df,collection_product_product_tag_df.category_id==collection_category_df.id,'left')
  collection_product_product_tag_df=duplicate_col(collection_product_product_tag_df)
  collection_product_product_tag_df=collection_product_product_tag_df.select(['product_id','tag_name','name'])
  collection_product_product_tag_df=collection_product_product_tag_df.select(concat_ws(' ',collection_product_product_tag_df.name,collection_product_product_tag_df.tag_name).alias('collection_tag'),'product_id')
  data=(collection_product_product_tag_df.groupby('product_id').pivot('collection_tag').count())
  data=data.na.fill(0)
  data_df=data.toPandas()
  data_df=data_df.set_index('product_id')
  data_df=string_column(data_df)
  columns=data_df.columns
  data_df['tag_string']=data_df[columns].apply(lambda row: ' '.join(row.values.astype(str)), axis=1)
  data_df=data_df.reset_index()
  print(data_df.head())
  return data_df



def duplicate_col(data=None):
  """
    Duplicate columns in a DataFrame by appending '_duplicate_' and the index to the column name.

    Parameters:
    - data (pd.DataFrame): The input DataFrame with columns to be duplicated.

    Returns:
    pd.DataFrame: A new DataFrame with duplicated columns.
    """
  df_cols = data.columns
  duplicate_col_index = [idx for idx,
  val in enumerate(df_cols) if val in df_cols[:idx]]
  for i in duplicate_col_index:
    df_cols[i] = df_cols[i] + '_duplicate_'+ str(i)
  data = data.toDF(*df_cols)
  return data





def collection_tag_join_df(collection_category_df,collection_category_tag_df,tag_cloud_df,product_tag_df,feature_list,join_type):
  product_product_tag_df=product_tag_df.join(tag_cloud_df,product_tag_df.tag_id==tag_cloud_df.id,'left')
  product_product_tag_df=product_product_tag_df.filter(product_product_tag_df['tag_id'].isin(feature_list['tag_cloud']))
  collection_product_product_tag_df=product_product_tag_df.join(collection_category_tag_df,collection_category_tag_df.tag_id==product_product_tag_df.tag_id,'left')
  collection_product_product_tag_df=collection_product_product_tag_df.join(collection_category_df,collection_product_product_tag_df.category_id==collection_category_df.id,'left')
  collection_product_product_tag_df.filter(collection_product_product_tag_df['category_id'].isin(feature_list['categoey_collection_tag_L']))
  collection_product_product_tag_df=duplicate_col(collection_product_product_tag_df)
  collection_product_product_tag_df=collection_product_product_tag_df.select(['product_id','tag_name','name'])
  collection_product_product_tag_df=collection_product_product_tag_df.select(concat_ws(' ',collection_product_product_tag_df.name,collection_product_product_tag_df.tag_name).alias('collection_tag'),'product_id')
  data=(collection_product_product_tag_df.groupby('product_id').pivot('collection_tag').count())
  data=data.na.fill(0)
  data_df=data.toPandas()
  data_df=data_df.set_index('product_id')
  data_df=string_column(data_df)
  columns=data_df.columns
  data_df['tag_string']=data_df[columns].apply(lambda row: ' '.join(row.values.astype(str)), axis=1)
  return data_df






def collection_FE_tag_join_df(collection_category_df,collection_category_tag_df,tag_cloud_df,product_tag_df,feature_list,join_type):
  product_product_tag_df=product_tag_df.join(tag_cloud_df,product_tag_df.tag_id==tag_cloud_df.id,'left')
  product_product_tag_df=product_product_tag_df.filter(product_product_tag_df['tag_id'].isin(feature_list['tag_cloud']))
  collection_product_product_tag_df=product_product_tag_df.join(collection_category_tag_df,collection_category_tag_df.tag_id==product_product_tag_df.tag_id,'left')
  collection_category_df=collection_category_df.filter(collection_category_df['id'].isin(feature_list['category_collection_FE']))
  collection_product_product_tag_df=collection_product_product_tag_df.join(collection_category_df,collection_product_product_tag_df.category_id==collection_category_df.id,'left')
  collection_product_product_tag_df.filter(collection_product_product_tag_df['category_id'].isin(feature_list['categoey_collection_tag_L']))
  collection_product_product_tag_df=duplicate_col(collection_product_product_tag_df)
  collection_product_product_tag_df=collection_product_product_tag_df.select(['product_id','tag_name','name'])
  collection_product_product_tag_df=collection_product_product_tag_df.select(concat_ws(' ',collection_product_product_tag_df.name,collection_product_product_tag_df.tag_name).alias('collection_tag'),'product_id')
  data=(collection_product_product_tag_df.groupby('product_id').pivot('collection_tag').count())
  data=data.na.fill(0)
  data_df=data.toPandas()
  data_df=data_df.set_index('product_id')
  data_df=string_column(data_df)
  columns=data_df.columns
  data_df['tag_string']=data_df[columns].apply(lambda row: ' '.join(row.values.astype(str)), axis=1)
  return data_df