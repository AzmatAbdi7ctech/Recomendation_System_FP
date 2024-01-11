def extract_data(spark,table_name=None,password=None,user_name=None,fetech_size=None,conn_url=None):

    """
    Extracts data from a specified database table using Apache Spark.

    Parameters:
    - spark (pyspark.sql.SparkSession): The Spark session variable.
    - table_name (str): The name of the database table from which to extract data.
    - password (str): The password for authenticating to the database.
    - user_name (str): The username for authenticating to the database.
    - fetch_size (int): The number of rows to fetch per batch during data extraction.
    - conn_url (str): The connection URL for the database.

    Returns:
    - pyspark.sql.DataFrame: A DataFrame containing the extracted data.
    """
    mysqlldf=spark.read \
    .format("jdbc") \
    .option("driver","com.mysql.jdbc.Driver") \
    .option("url",conn_url) \
    .option("dbtable",table_name) \
    .option("user",user_name) \
    .option("fetchSize",fetech_size )  \
    .option('password',password) \
    .load()
    return mysqlldf
