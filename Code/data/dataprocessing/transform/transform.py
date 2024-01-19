from pyspark.sql.functions import concat,concat_ws,round

def feature_list(df=None):
    """
    Extract feature lists from the input DataFrame.

    Parameters:
    - df (pyspark.sql.DataFrame): Input DataFrame containing feature columns.

    Returns:
    dict: A dictionary containing different feature lists.

    Feature Lists:
    - 'category_collection_FE': List of values extracted from the 'category_collection_FE' column.
    - 'categoey_collection_tag_L': List of values extracted from the 'category_collection_tag_L' column.
    - 'tag_cloud': List of values extracted from the 'tag_cloud' column.
    """
    # Extract values from 'category_collection_FE', 'category_collection_tag_L', and 'tag_cloud' columns
    category_collection_FE = [i[0] for i in df.select('category_collection_FE').collect()]
    category_collection_tag_L = [i[0] for i in df.select('category_collection_tag_L').collect()]
    tag_cloud = [i[0] for i in df.select('tag_cloud').collect()]

    # Return a dictionary containing the extracted feature lists
    return {
        'category_collection_FE': category_collection_FE,
        'categoey_collection_tag_L': category_collection_tag_L,
        'tag_cloud': tag_cloud
    }


def string_column(data_df):
    """
    Modify values in the input DataFrame by replacing non-empty and positive numeric values with the corresponding column name,
    and setting other values to an empty string.

    Parameters:
    - data_df (dict): A dictionary where keys are column names and values are lists of numeric or string values.

    Returns:
    dict: A modified dictionary where values in each column are replaced with the column name if the value is non-empty and positive,
    otherwise set to an empty string.

    Example:
    If input data_df is {'column1': ['3', '', '5'], 'column2': ['2', '-1', '']}, the output will be
    {'column1': ['column1', '', 'column1'], 'column2': ['column2', '', '']}.
    """
    for key, value in data_df.items():
        data_df[key] = [key if (v != '' and int(v) > 0) else '' for v in value]

    return data_df


def tag_cloud_join_df(collection_category_df, collection_category_tag_df, tag_cloud_df, product_tag_df, feature_list, join_type):
    """
    Perform a series of joins and transformations on the provided DataFrames to create a new DataFrame representing tag-cloud-related information.

    Parameters:
    - collection_category_df (pyspark.sql.DataFrame): DataFrame containing collection category information.
    - collection_category_tag_df (pyspark.sql.DataFrame): DataFrame containing collection category tag information.
    - tag_cloud_df (pyspark.sql.DataFrame): DataFrame containing tag cloud information.
    - product_tag_df (pyspark.sql.DataFrame): DataFrame containing product tag information.
    - feature_list (dict): A dictionary containing feature lists, including 'tag_cloud'.
    - join_type (str): Type of join to be used in DataFrame joins.

    Returns:
    pandas.DataFrame: A DataFrame representing tag-cloud-related information for products.

    Note:
    - The function performs left joins and filtering based on feature_list['tag_cloud'] values.
    - It creates a new DataFrame with tag-related information for each product.
    - The final DataFrame includes a 'tag_string' column representing a concatenated string of tag information for each product.
    """
    product_product_tag_df = product_tag_df.join(tag_cloud_df, product_tag_df.tag_id == tag_cloud_df.id, 'left')
    product_product_tag_df = product_product_tag_df.filter(product_product_tag_df['tag_id'].isin(feature_list['tag_cloud']))

    collection_product_product_tag_df = product_product_tag_df.join(collection_category_tag_df, collection_category_tag_df.tag_id == product_product_tag_df.tag_id, 'left')
    collection_product_product_tag_df = collection_product_product_tag_df.join(collection_category_df, collection_product_product_tag_df.category_id == collection_category_df.id, 'left')
    collection_product_product_tag_df = duplicate_col(collection_product_product_tag_df)
    collection_product_product_tag_df = collection_product_product_tag_df.select(['product_id', 'tag_name', 'name'])
    collection_product_product_tag_df = collection_product_product_tag_df.select(concat_ws(' ', collection_product_product_tag_df.name, collection_product_product_tag_df.tag_name).alias('collection_tag'), 'product_id')

    data = (collection_product_product_tag_df.groupby('product_id').pivot('collection_tag').count())
    data = data.na.fill(0)
    data_df = data.toPandas()
    data_df = data_df.set_index('product_id')
    data_df = string_column(data_df)
    columns = data_df.columns
    data_df['tag_string'] = data_df[columns].apply(lambda row: ' '.join(row.values.astype(str)), axis=1)
    data_df = data_df.reset_index()

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





def collection_tag_join_df(collection_category_df, collection_category_tag_df, tag_cloud_df, product_tag_df, feature_list, join_type):
    """
    Perform a series of joins and transformations on the provided DataFrames to create a new DataFrame representing collection tag-related information.

    Parameters:
    - collection_category_df (pyspark.sql.DataFrame): DataFrame containing collection category information.
    - collection_category_tag_df (pyspark.sql.DataFrame): DataFrame containing collection category tag information.
    - tag_cloud_df (pyspark.sql.DataFrame): DataFrame containing tag cloud information.
    - product_tag_df (pyspark.sql.DataFrame): DataFrame containing product tag information.
    - feature_list (dict): A dictionary containing feature lists, including 'tag_cloud' and 'categoey_collection_tag_L'.
    - join_type (str): Type of join to be used in DataFrame joins.

    Returns:
    pandas.DataFrame: A DataFrame representing collection tag-related information for products.

    Note:
    - The function performs left joins and filtering based on feature_list['tag_cloud'] and feature_list['categoey_collection_tag_L'] values.
    - It creates a new DataFrame with collection tag-related information for each product.
    - The final DataFrame includes a 'tag_string' column representing a concatenated string of tag information for each product.
    """
    product_product_tag_df = product_tag_df.join(tag_cloud_df, product_tag_df.tag_id == tag_cloud_df.id, 'left')
    product_product_tag_df = product_product_tag_df.filter(product_product_tag_df['tag_id'].isin(feature_list['tag_cloud']))

    collection_product_product_tag_df = product_product_tag_df.join(collection_category_tag_df, collection_category_tag_df.tag_id == product_product_tag_df.tag_id, 'left')
    collection_product_product_tag_df = collection_product_product_tag_df.join(collection_category_df, collection_product_product_tag_df.category_id == collection_category_df.id, 'left')
    collection_product_product_tag_df = collection_product_product_tag_df.filter(collection_product_product_tag_df['category_id'].isin(feature_list['categoey_collection_tag_L']))
    collection_product_product_tag_df = duplicate_col(collection_product_product_tag_df)
    collection_product_product_tag_df = collection_product_product_tag_df.select(['product_id', 'tag_name', 'name'])
    collection_product_product_tag_df = collection_product_product_tag_df.select(concat_ws(' ', collection_product_product_tag_df.name, collection_product_product_tag_df.tag_name).alias('collection_tag'), 'product_id')

    data = (collection_product_product_tag_df.groupby('product_id').pivot('collection_tag').count())
    data = data.na.fill(0)
    data_df = data.toPandas()
    data_df = data_df.set_index('product_id')
    data_df = string_column(data_df)
    columns = data_df.columns
    data_df['tag_string'] = data_df[columns].apply(lambda row: ' '.join(row.values.astype(str)), axis=1)
    data_df = data_df.reset_index()
    return data_df






def collection_FE_tag_join_df(collection_category_df, collection_category_tag_df, tag_cloud_df, product_tag_df, feature_list, join_type):
    """
    Perform a series of joins and transformations on the provided DataFrames to create a new DataFrame representing
    collection feature and tag-related information.

    Parameters:
    - collection_category_df (pyspark.sql.DataFrame): DataFrame containing collection category information.
    - collection_category_tag_df (pyspark.sql.DataFrame): DataFrame containing collection category tag information.
    - tag_cloud_df (pyspark.sql.DataFrame): DataFrame containing tag cloud information.
    - product_tag_df (pyspark.sql.DataFrame): DataFrame containing product tag information.
    - feature_list (dict): A dictionary containing feature lists, including 'tag_cloud', 'category_collection_FE', and 'categoey_collection_tag_L'.
    - join_type (str): Type of join to be used in DataFrame joins.

    Returns:
    pandas.DataFrame: A DataFrame representing collection feature and tag-related information for products.

    Note:
    - The function performs left joins and filtering based on feature_list values.
    - It creates a new DataFrame with collection feature and tag-related information for each product.
    - The final DataFrame includes a 'tag_string' column representing a concatenated string of tag information for each product.
    """
    product_product_tag_df = product_tag_df.join(tag_cloud_df, product_tag_df.tag_id == tag_cloud_df.id, 'left')
    product_product_tag_df = product_product_tag_df.filter(product_product_tag_df['tag_id'].isin(feature_list['tag_cloud']))

    collection_product_product_tag_df = product_product_tag_df.join(collection_category_tag_df, collection_category_tag_df.tag_id == product_product_tag_df.tag_id, 'left')
    collection_category_df = collection_category_df.filter(collection_category_df['id'].isin(feature_list['category_collection_FE']))
    collection_product_product_tag_df = collection_product_product_tag_df.join(collection_category_df, collection_product_product_tag_df.category_id == collection_category_df.id, 'left')
    collection_product_product_tag_df = collection_product_product_tag_df.filter(collection_product_product_tag_df['category_id'].isin(feature_list['categoey_collection_tag_L']))
    collection_product_product_tag_df = duplicate_col(collection_product_product_tag_df)
    collection_product_product_tag_df = collection_product_product_tag_df.select(['product_id', 'tag_name', 'name'])
    collection_product_product_tag_df = collection_product_product_tag_df.select(concat_ws(' ', collection_product_product_tag_df.name, collection_product_product_tag_df.tag_name).alias('collection_tag'), 'product_id')

    data = (collection_product_product_tag_df.groupby('product_id').pivot('collection_tag').count())
    data = data.na.fill(0)
    data_df = data.toPandas()
    data_df = data_df.set_index('product_id')
    data_df = string_column(data_df)
    columns = data_df.columns
    data_df['tag_string'] = data_df[columns].apply(lambda row: ' '.join(row.values.astype(str)), axis=1)
    data_df = data_df.reset_index()
    return data_df


def cosin_transformation(df):
    try:
        df=df.select('*',round('cosin_score',3).alias('cosin_score_1')).withColumnRenamed('Product_A','original_product').withColumnRenamed('Product_B','matched_product')
    # print(df.columns())
    # df.rename(columns={'Product_A':'original_product'})
    # df=df['cosin_score'].round(2) 
    except:
        print('pandas dataframe')
    df=df.toPandas()
    # print(df.head())
    # df=df.rename(columns={'cosin_score_1':'cosin_score'})
    print(df.columns)
    df=df[['original_product','matched_product','cosin_score_1']]
    df=df.rename(columns={'cosin_score_1':'cosin_score'})
    return df