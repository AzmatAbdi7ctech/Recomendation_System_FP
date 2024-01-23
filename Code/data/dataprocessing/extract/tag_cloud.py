from pyspark.sql.functions import date_format

def tag_cloud_transformation(df=None, column_list=None):
    """
    Transform the input DataFrame by selecting specific columns and converting it to a Pandas DataFrame.

    Parameters:
    - df (pyspark.sql.DataFrame): Input DataFrame containing the data.
    - column_list (list): List of columns to select from the DataFrame.

    Returns:
    pandas.DataFrame: Transformed DataFrame with selected columns.

    Note:
    - The function may filter out rows where 'status' or 'status_for_sale' is equal to 2 (uncomment if needed).
    - The selected columns include 'id' and 'tag_name'.
    """
    # Uncomment the following line if filtering logic needs to be applied
    # df = df.filter((df.status != 2) & (df.status_for_sale != 2))

    # Select the specified columns
    # columns = ['id', 'tag_name']
    df = df.select(column_list)

    # Convert the DataFrame to a Pandas DataFrame
    df = df.toPandas()

    return df
