from sentence_transformers import SentenceTransformer, util
import pandas as pd

def model_load(model_name):
    return  SentenceTransformer(model_name)
def model_train(df,model_name):
    product_tag_string=[ string[0].lower() for string in df.select('tag_string').collect()]
    product_tag_id=[ id[0] for id in df.select('product_id').collect()]
    model =model_load(model_name)
    embeddings = model.encode(product_tag_string, convert_to_tensor=True)
    cos_sim = util.cos_sim(embeddings, embeddings)
    all_product_combination = {'cosin_score':[] , 'Product_A':[] , 'Product_B':[]}
    for i in range(len(cos_sim)-1):
        for j in range(i+1, len(cos_sim)):
            all_product_combination['cosin_score'].append(float(cos_sim[i][j]))
            all_product_combination['Product_A'].append(product_tag_id[i])
            all_product_combination['Product_B'].append(product_tag_id[j])
    return all_product_combination



def model_artifact(df,version):
    return df.filter(df['model_artifact']==version).select('model_name').collect()[0][0]
