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