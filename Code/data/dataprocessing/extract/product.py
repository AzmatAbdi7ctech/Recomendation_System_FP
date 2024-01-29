from pyspark.sql.functions import date_format

def product_transformation(df=None, column_list=None):
    """
    Transform the input DataFrame by filtering rows based on 'status' and 'status_for_sale' values,
    selecting specific columns, and formatting date columns.

    Parameters:
    - df (pyspark.sql.DataFrame): Input DataFrame containing the data.
    - column_list (list): List of columns to select from the DataFrame.

    Returns:
    pandas.DataFrame: Transformed DataFrame with filtered rows, selected columns, and formatted date columns.

    Note:
    - The function filters out rows where 'status' or 'status_for_sale' is equal to 2.
    - Date columns 'published_at', 'created_at', and 'updated_at' are formatted to 'yyyy-MM-dd HH:mm:ss'.
    - The selected columns include 'product_id', 'product_title', 'published_at', 'product_thumbnail', 'created_at', and 'updated_at'.
    """
    # Filter rows based on status and status_for_sale
    df = df.filter((df.status != 2) | (df.status_for_sale != 2))

    # Select the specified columns
    # columns = ['product_id', 'product_title', 'published_at', 'product_thumbnail', 'created_at', 'updated_at']
    df = df.select(column_list)

    # Format date columns
    # df = df.withColumn("published_at", date_format("published_at", "yyyy-MM-dd HH:mm:ss"))
    # df = df.withColumn("created_at", date_format("created_at", "yyyy-MM-dd HH:mm:ss"))
    # df = df.withColumn("updated_at", date_format("updated_at", "yyyy-MM-dd HH:mm:ss"))

    # Convert the DataFrame to a Pandas DataFrame
    df = df.toPandas()

    return df
