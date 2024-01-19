from Code.utils.libraries import * 
from Code.data.dataprocessing.load.load import *
from Code.data.dataprocessing.transform.transform import *
from sessions import *
from pyspark.sql.functions import date_format
from Code.models.model_training.model import *
from Code.utils.logs import *
BASE_PATH=os.getenv('BASEPATH')
CONN_URL = os.getenv('CONN_URL')
PASSWORD = os.getenv('PASSWORD')
USERNAME=os.getenv('USER_NAME')
configur = ConfigParser() 
configur.read(BASE_PATH+'config.ini')
spark_session=session(configur.get('driver_path_pyspark','pyspark'))
cs1=pd.read_csv(f"{configur.get('cosin_similarity_base_path','path')}{configur.get('cosin_similarity_base_path','cs1')}")#load_csv(spark_session,file_path=configur.get('cosin_similarity_base_path','cs1'))
cs2=pd.read_csv(f"{configur.get('cosin_similarity_base_path','path')}{configur.get('cosin_similarity_base_path','cs2')}")#load_csv(spark=spark_session,file_path=configur.get('cosin_similarity_base_path','cs2'))
cs3=pd.read_csv(f"{configur.get('cosin_similarity_base_path','path')}{configur.get('cosin_similarity_base_path','cs3')}")#Sload_csv(spark=spark_session,file_path=configur.get('cosin_similarity_base_path','cs3'))
cosin_df=[]
product_ids=[8692,2178,8687,1087,3069]#cs1.Product_A.unique()#[id[0] for id in cs1.select('Product_A').distinct().collect()]  in pyspark [8692,2178,8687,1087,3069]
logging.info(f"total number of product {len(product_ids)}")
st = time.time()
for r in product_ids:
    print(r)
    cs1_product_b=[]
    cs2_product_b=[]
    cs3_product_b=[]
    cs1_product_b=cs1[(cs1['Product_A']==r) & (cs1['cosin_score']>=.80)]['Product_B'].unique().tolist()  #[ product[0] for product in cs1.filter((cs1['Product_A']==r) & (cs1['cosin_score']>=.80))['Product_B'].unique()]
    cs2_product_b=cs2[(cs2['Product_A']==r) & (cs2['cosin_score']>=.85)]['Product_B'].unique().tolist()#values
    cs3_product_b=cs3[(cs3['Product_A']==r) & (cs3['cosin_score']>=.90)]['Product_B'].unique().tolist()#values
    # cs2temp=cs2.filter((cs2['Product_A']==r) & (cs2['cosin_score']>=.85))
    # cs3temp=cs3.filter((cs3['Product_A']==r) & (cs3['cosin_score']>=.90))

    # for row in  cs1temp.collect():
    #     if float(row['cosin_score'])>=.80:
    #         cs1_product_b.append(row['Product_B'])
        
    # for row2 in cs2temp.collect():
    #     if float(row2['cosin_score'])>=.85:
    #         cs1_product_b.append(row2['Product_B'])
    #         # cs2_product_b.append(row2['Product_B'])
    # for row3 in cs3temp.collect():
    #     if float(row3['cosin_score'])>=.90 :
    #         cs1_product_b.append(row3['Product_B'])
                # cs3_product_b.append(row3['Product_B'])

#print(cs1_product_b,cs2_product_b,cs3_product_b)
    for id in cs2_product_b:
        cs1_product_b.append(id)
    for id in cs3_product_b:
        cs1_product_b.append(id)
    # final_list=cs1_product_b+cs2_product_b+cs3_product_b
    my_dict = {i:cs1_product_b.count(i) for i in cs1_product_b}

    top_recomendation=[]
    for key,value in my_dict.items():
        if value >=2:
            top_recomendation.append(key)
    # print('top re',top_recomendation)
    cosin_df.append(cs1[(cs1['Product_A']==r) & (cs1['Product_B'].isin(top_recomendation))])
    # cosin_df.append(cs1.filter((cs1['Product_A']==r) & (cs1['Product_B'].isin(top_recomendation))))  # in pandas

df=cosin_df[0]
print(len(cosin_df[1:]))
for i in cosin_df[1:]:
    try:
         #df=df.union(i) #in pyspark
         df=df._append(i,ignore_index = True) #in pandas
    except:
        logging.info('out of index')
et = time.time()
logging.info(f'Execution time:{et-st} seconds')
dump_csv(df,configur.get('cosin_similarity_base_path','path'),table_name=f"cosin_similarity_{datetime.date.today()}")
df=load_csv(spark_session,file_path=f"{configur.get('cosin_similarity_base_path','path')}cosin_similarity_{datetime.date.today()}.csv")#f"{configur.get('cosin_similarity_base_path','path')}cosin_similarity_{datetime.date.today()}.csv")
df=cosin_transformation(df)
dump_csv(df,file_path=configur.get('cosin_similarity_base_path','path'), table_name=f"cosin_similarity_{datetime.date.today()}")


