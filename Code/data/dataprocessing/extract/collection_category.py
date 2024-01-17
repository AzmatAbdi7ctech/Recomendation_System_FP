from pyspark.sql.functions import date_format
def collection_category_transformation(df=None, column_list=None):
    """
    Transform the input DataFrame by selecting specific columns and filtering out rows based on 'status' and 'status_for_sale' values.

    Parameters:
    - df (pyspark.sql.DataFrame): Input DataFrame containing the data.
    - column_list (list): List of columns to select from the DataFrame.

    Returns:
    pandas.DataFrame: Transformed DataFrame with selected columns.

    Note:
    - The function filters out rows where 'status' or 'status_for_sale' is equal to 2.
    - The selected columns include 'id', 'name', and 'parent_id'.
    """
    # Uncomment the following line if the filtering logic needs to be applied
    # df = df.filter((df.status != 2) & (df.status_for_sale != 2))

    # Select the specified columns
    df = df.select(column_list)

    # Convert the DataFrame to a Pandas DataFrame
    df = df.toPandas()

    return df
