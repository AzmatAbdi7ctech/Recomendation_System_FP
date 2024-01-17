from Code.utils.libraries import * 
from Code.data.dataprocessing.load.load import *
from Code.data.dataprocessing.transform.transform import *
from sessions import *
from pyspark.sql.functions import date_format
from Code.models.model_training.model import *


BASE_PATH=os.getenv('BASEPATH')
CONN_URL = os.getenv('CONN_URL')
PASSWORD = os.getenv('PASSWORD')
USERNAME=os.getenv('USER_NAME')
configur = ConfigParser() 
configur.read(BASE_PATH+'config.ini')
spark_session=session(configur.get('driver_path_pyspark','pyspark'))
cs1=load_csv(spark_session,file_path=configur.get('cosin_similarity_base_path','cs1'))
cs2=load_csv(spark=spark_session,file_path=configur.get('cosin_similarity_base_path','cs2'))
cs3=load_csv(spark=spark_session,file_path=configur.get('cosin_similarity_base_path','cs3'))

cosin_df=[]

product_ids=[id[0] for id in cs1.select('Product_A').distinct().collect()]

for r in product_ids:
    print(r)
    cs1_product_b=[]
    cs2_product_b=[]
    cs3_product_b=[]

    cs1temp=cs1.filter(cs1['Product_A']==r)
    cs2temp=cs2.filter(cs2['Product_A']==r)
    cs3temp=cs3.filter(cs3['Product_A']==r)

    for row in  cs1temp.collect():
        if float(row['cosin_score'])>=.80:
            cs1_product_b.append(row['Product_B'])
        
    for row2 in cs2temp.collect():
        if float(row2['cosin_score'])>=.85:
            cs1_product_b.append(row2['Product_B'])
            # cs2_product_b.append(row2['Product_B'])
    for row3 in cs3temp.collect():
        if float(row3['cosin_score'])>=.90 :
            cs1_product_b.append(row3['Product_B'])
                # cs3_product_b.append(row3['Product_B'])

#print(cs1_product_b,cs2_product_b,cs3_product_b)
    my_dict = {i:cs1_product_b.count(i) for i in cs1_product_b}

    top_recomendation=[]
    for key,value in my_dict.items():
        if value >=2:
            top_recomendation.append(key)
    cosin_df.append(cs1.filter((cs1['Product_A']==r) & (cs1['Product_B'].isin(top_recomendation))))

df=cosin_df[0]
print(len(cosin_df[1:]))
for i in cosin_df[1:]:
    try:
        df=df.union(i)
    except:
        print('out of index')
df.toPandas().to_csv('cosin_simmilarity1.csv')




