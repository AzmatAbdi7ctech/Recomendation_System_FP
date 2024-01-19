from sentence_transformers import SentenceTransformer, util
import pandas as pd

def model_load(model_name):
    """
    Load a pre-trained SentenceTransformer model.

    Parameters:
    - model_name (str): The name or path of the pre-trained SentenceTransformer model.

    Returns:
    sentence_transformers.SentenceTransformer: A pre-trained SentenceTransformer model.

    Example:
    ```
    model_name = 'bert-base-nli-mean-tokens'
    loaded_model = model_load(model_name)
    ```
    """
    return SentenceTransformer(model_name)


def model_train(df=None, model_name=None):
    """
    Train a product similarity model using a pre-trained SentenceTransformer model.

    Parameters:
    - df (pyspark.sql.DataFrame): DataFrame containing product information with 'tag_string' and 'product_id' columns.
    - model_name (str): The name or path of the pre-trained SentenceTransformer model.

    Returns:
    pandas.DataFrame: DataFrame containing product similarity information with 'cosin_score', 'Product_A', and 'Product_B' columns.

    Example:
    ```
    model_name = 'bert-base-nli-mean-tokens'
    similarity_df = model_train(product_df, model_name)
    ```

    Note:
    - The function extracts 'tag_string' and 'product_id' columns from the input DataFrame.
    - It loads a pre-trained SentenceTransformer model specified by 'model_name'.
    - Embeddings are generated for product tag strings using the loaded model.
    - Cosine similarity scores are calculated between all pairs of product embeddings.
    - The result is returned as a DataFrame with similarity information.
    """
    product_tag_string = [string[0].lower() for string in df.select('tag_string').collect()]
    product_tag_id = [id[0] for id in df.select('product_id').collect()]
    
    model = model_load(model_name)
    embeddings = model.encode(product_tag_string, convert_to_tensor=True)
    cos_sim = util.cos_sim(embeddings, embeddings)
    
    all_product_combination = {'cosin_score': [], 'Product_A': [], 'Product_B': []}
    
    for i in range(len(cos_sim)-1):
        for j in range(i+1, len(cos_sim)):
            all_product_combination['cosin_score'].append(float(cos_sim[i][j]))
            all_product_combination['Product_A'].append(product_tag_id[i])
            all_product_combination['Product_B'].append(product_tag_id[j])
    
    return pd.DataFrame(all_product_combination)



def model_artifact(df=None, version=None):
    """
    Retrieve the model name associated with a specific version from the input DataFrame.

    Parameters:
    - df (pyspark.sql.DataFrame): DataFrame containing model artifact information with 'model_artifact' and 'model_name' columns.
    - version (str): The version of the model artifact to retrieve.

    Returns:
    str: The model name associated with the specified version.

    Example:
    ```
    version = 'v1.0'
    model_name = model_artifact(artifact_df, version)
    ```

    Note:
    - The function filters the input DataFrame based on the provided version.
    - It selects the 'model_name' column from the filtered DataFrame and retrieves the first row.
    - The model name associated with the specified version is returned.
    """
    return df.filter(df['model_artifact'] == version).select('model_name').collect()[0][0]

