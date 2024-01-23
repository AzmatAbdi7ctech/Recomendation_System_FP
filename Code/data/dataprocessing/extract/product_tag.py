from pyspark.sql.functions import date_format

def producttag_transformation(df=None, column_list=None):
    """
    Transform the input DataFrame by formatting date columns and converting it to a Pandas DataFrame.

    Parameters:
    - df (pyspark.sql.DataFrame): Input DataFrame containing the data.
    - column_list (list): List of columns to select from the DataFrame.

    Returns:
    pandas.DataFrame: Transformed DataFrame with formatted date columns.

    Note:
    - The function may filter out rows where 'status' or 'status_for_sale' is equal to 2 (uncomment if needed).
    - Date columns 'created_at' and 'updated_at' are formatted to 'yyyy-MM-dd HH:mm:ss'.
    - The selected columns can be specified using the 'column_list' parameter.
    """
    # Uncomment the following line if filtering logic needs to be applied
    # df = df.filter((df.status != 2) & (df.status_for_sale != 2))

    # Format date columns
    df = df.withColumn("created_at", date_format("created_at", "yyyy-MM-dd HH:mm:ss"))
    df = df.withColumn("updated_at", date_format("updated_at", "yyyy-MM-dd HH:mm:ss"))

    # Select the specified columns
    try:
        df = df.select(column_list)
    except: 
        print("columns is not given")

    # Convert the DataFrame to a Pandas DataFrame
    df = df.toPandas()

    return df
