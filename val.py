from Code.utils.libraries import * 
from Code.data.dataprocessing.load.load import *
from Code.data.dataprocessing.transform.transform import *
from sessions import *
from pyspark.sql.functions import date_format
from Code.models.model_training.model import *
from Code.utils.logs import *

confing_name='config.ini'
BASE_PATH=os.getcwd()
configur = ConfigParser() 
configur.read(os.path.join(BASE_PATH,'Code',confing_name)  ) #for dev
spark_session=session(configur.get('driver_path_pyspark','pyspark'))
databse_csv_path=f"{BASE_PATH}/Code/Database/"

cosin_path=str(os.path.join(BASE_PATH,'Code','cosin_similarity_registry')).replace('\\', '/')
spark_session=session(configur.get('driver_path_pyspark','pyspark'))

th1=.75
th2=.75
th3=.75
val_path=f"{cosin_path}/{str(time.strftime('%Y_%m_%d'))}/"  
cosin_path1=f"{cosin_path}/{str(time.strftime('%Y_%m_%d'))}/{configur.get('cosin_similarity_base_path','cs1')}"
cosin_path2=f"{cosin_path}/{str(time.strftime('%Y_%m_%d'))}/{configur.get('cosin_similarity_base_path','cs1')}"
cosin_path3=f"{cosin_path}/{str(time.strftime('%Y_%m_%d'))}/{configur.get('cosin_similarity_base_path','cs1')}"
Product_df=load_csv(spark_session,databse_csv_path,configur.get('dataframe','product'))
logging.info('cosin csv started reading')
cs1=pd.read_csv(cosin_path1)#load_csv(spark_session,file_path=configur.get('cosin_similarity_base_path','cs1'))
cs2=pd.read_csv(cosin_path2)#load_csv(spark=spark_session,file_path=configur.get('cosin_similarity_base_path','cs2'))
cs3=pd.read_csv(cosin_path3)#Sload_csv(spark=spark_session,file_path=configur.get('cosin_similarity_base_path','cs3'))
logging.info('cosin csv ended reading')
cosin_df=[]

product_ids=cs1.Product_A.unique()#cs1.Product_A.unique()#[id[0] for id in cs1.select('Product_A').distinct().collect()]#[8692,2178,8687,1087,3069]#cs1.Product_A.unique()#[id[0] for id in cs1.select('Product_A').distinct().collect()]  in pyspark 
print("total number of product",len(product_ids))
st = time.time()
for r in product_ids:
    print(r)
    cs1_product_b=[]
    cs2_product_b=[]
    cs3_product_b=[]
    try:
        cs1_product_b=cs1[(cs1['Product_A']==r) & (cs1['cosin_score']>=th1)]['Product_B'].unique().tolist()  #[ product[0] for product in cs1.filter((cs1['Product_A']==r) & (cs1['cosin_score']>=.80))['Product_B'].unique()]
        cs2_product_b=cs2[(cs2['Product_A']==r) & (cs2['cosin_score']>=th2)]['Product_B'].unique().tolist()#values
        cs3_product_b=cs3[(cs3['Product_A']==r) & (cs3['cosin_score']>=th3)]['Product_B'].unique().tolist()#values
    except:
        logging.info('product does not exit in csv')

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

for i in cosin_df[1:]:
    try:
         #df=df.union(i) #in pyspark
         df=df._append(i,ignore_index = True) #in pandas
    except:
         print('out of index')
#df.toPandas().to_csv('cosin_simmilarity1.csv')
et = time.time()
logging.info(f'Execution time:{et-st} seconds')

logging.info('valdation csv part done')
dump_csv(df,val_path,table_name=f"cosin_similarity_{datetime.date.today()}")
df=load_csv(spark_session,file_path=val_path,file_name=f"cosin_similarity_{datetime.date.today()}")#f"{configur.get('cosin_similarity_base_path','path')}cosin_similarity_{datetime.date.today()}.csv")
df=cosin_transformation(df)

df=date_difference(Product_df,df)

dump_csv(df,val_path,table_name=f"cosin_similarity_{datetime.date.today()}")
print(df.head())
logging.info(f'validation csv saved {val_path} cosin_similarity_{datetime.date.today()}')




