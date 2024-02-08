def load_csv(spark=None, file_path=None,file_name=None ,format='csv'):
    """
    Load a CSV file into a Spark DataFrame.

    Parameters:
    - spark (pyspark.sql.SparkSession): The Spark session used for creating the DataFrame.
    - file_path (str): The path to the CSV file to be loaded.
    - format (str, optional): The format of the file. Default is 'csv'.

    Returns:
    pyspark.sql.DataFrame: The Spark DataFrame containing the loaded data.

    Raises:
    ValueError: If an unsupported format is provided.
    """
    print(file_path,file_name)
    if format.lower() == 'csv':
        df = spark.read.format(format).option('header', True).load(f'{file_path}{file_name}.csv')
        return df
    else:
        raise ValueError("Unsupported format. Currently, only 'csv' format is supported.")

def dump_csv(df=None, file_path=None, format='csv',table_name=None):
    """
    Dump a Spark DataFrame to a CSV file.

    Parameters:
    - df (pyspark.sql.DataFrame): The input Spark DataFrame to be saved.
    - file_path (str): The path where the CSV file will be saved.
    - format (str, optional): The format in which to save the DataFrame. Default is 'csv'.
    - table_name (str) save the table with the name

    Returns:
    None

    Raises:
    ValueError: If an unsupported format is provided.
    """

    if format.lower() == 'csv':
        
        # df.to_csv(f'{file_path+table_name}.csv', index=False)  for dev
        df.to_csv(f'{file_path}{table_name}.csv',index=False)   #for stagging
        print("file saved",table_name)
    else:
        raise ValueError("Unsupported format. Currently, only 'csv' format is supported.")
    

