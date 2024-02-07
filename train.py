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
databse_csv_path=f"{BASE_PATH}/Code/Database/"
spark_session=session(configur.get('driver_path_pyspark','pyspark'))
feature_list_path=str(os.path.join(BASE_PATH,'Code','feature_registry')).replace('\\', '/')
model_path=str(os.path.join(BASE_PATH,'Code','models','model_artifact')).replace('\\', '/')
transformation_csv_path=f"{BASE_PATH}/{configur.get('product_transformation','path')}"
cosin_path=str(os.path.join(BASE_PATH,'Code','cosin_similarity_registry')).replace('\\', '/')

try:
    os.makedirs(feature_list_path)
    print(feature_list_path)
except:
        logging.info(f"FOLDER ALREADY EXIST{feature_list_path}")
        
try:
    os.makedirs(model_path)
    print(model_path)
except:
        logging.info(f"FOLDER ALREADY EXIST{model_path}")
     
try:
    os.makedirs(f"{BASE_PATH}/{configur.get('product_transformation','path')}")
    print(transformation_csv_path)
except:
        logging.info(f"FOLDER ALREADY EXIST{BASE_PATH}/{configur.get('product_transformation','path')}")


try:
    # cosin_path=f'{BASE_PATH}/{configur.get("cosin_similarity_base_path","path")}{str(time.strftime("%Y_%m_%d"))}'
    os.mkdir(cosin_path)
    print(cosin_path)
except:
        logging.info(f'FOLDER ALREADY EXIST {cosin_path}')

cosin_path=f'{cosin_path}/{str(time.strftime("%Y_%m_%d"))}'
try:
    # cosin_path=f'{BASE_PATH}/{configur.get("cosin_similarity_base_path","path")}{str(time.strftime("%Y_%m_%d"))}'
    os.mkdir(cosin_path)
    print(cosin_path)
except:
        logging.info(f'FOLDER ALREADY EXIST {cosin_path}')
cosin_path=cosin_path+'/'
Features_clothing_df=load_csv(spark_session,f'{feature_list_path}/',configur.get('feature_list','feature_list_clothing'))
Features_accesories_df=load_csv(spark_session,f'{feature_list_path}/',configur.get('feature_list','feature_list_accessories'))
Product_df=load_csv(spark_session,databse_csv_path,configur.get('dataframe','product'))
Collection_Category_df=load_csv(spark_session,databse_csv_path,configur.get('dataframe','collection_category'))
Collection_Category_tag_df=load_csv(spark_session,databse_csv_path,configur.get('dataframe','collection_category_tag'))
Product_tag_df=load_csv(spark_session,databse_csv_path,configur.get('dataframe','product_tag'))
Tag_cloud_df=load_csv(spark_session,databse_csv_path,configur.get('dataframe','tag_cloud'))
feature_clothing=feature_list(Features_clothing_df)
feature_accesories=feature_list(Features_accesories_df)
model_artifact_df=load_csv(spark_session,model_path+'/',configur.get('model','model_artifact_path'))





#--clothing--#
logging.info (f'clothing transformation tag_cloud started')
dump_csv(tag_cloud_join_df(Collection_Category_df,Collection_Category_tag_df,
                           Tag_cloud_df,Product_tag_df,feature_clothing,'left'),
                           file_path=transformation_csv_path,
                           table_name=configur.get('product_transformation','product_t1_clothing'))
logging.info (f'clothing transformation tag_cloud ended')
logging.info (f'clothing transformation collection_tag started')
dump_csv(collection_tag_join_df(Collection_Category_df,Collection_Category_tag_df,
                           Tag_cloud_df,Product_tag_df,feature_clothing,'left'),
                           file_path=transformation_csv_path,
                           table_name=configur.get('product_transformation','product_t2_clothing'))
logging.info (f'clothing transformation collection_tag ended')
logging.info (f'clothing transformation collection_FE_tag started')
dump_csv(collection_FE_tag_join_df(Collection_Category_df,Collection_Category_tag_df,
                           Tag_cloud_df,Product_tag_df,feature_clothing,'left'),
                           file_path=transformation_csv_path,
                           table_name=configur.get('product_transformation','product_t3_clothing'))
logging.info (f'clothing transformation collection_FE_tag ended')
#--clothing--#




#--accesories--#
logging.info (f'accesories transformation tag_cloud started')
dump_csv(tag_cloud_join_df(Collection_Category_df,Collection_Category_tag_df,
                           Tag_cloud_df,Product_tag_df,feature_accesories,'left'),
                           file_path=transformation_csv_path,
                           table_name=configur.get('product_transformation','product_t1_accessories'))
logging.info (f'accesories transformation tag_cloud ended')

logging.info (f'accesories transformation collection_tag started')
dump_csv(collection_tag_join_df(Collection_Category_df,Collection_Category_tag_df,
                           Tag_cloud_df,Product_tag_df,feature_accesories,'left'),
                           file_path=transformation_csv_path,
                           table_name=configur.get('product_transformation','product_t2_accessories'))
logging.info (f'accesories transformation collection_tag ended')

logging.info (f'accesories transformation collection_FE_tag stated')
dump_csv(collection_FE_tag_join_df(Collection_Category_df,Collection_Category_tag_df,
                           Tag_cloud_df,Product_tag_df,feature_accesories,'left'),
                           file_path=transformation_csv_path,
                           table_name=configur.get('product_transformation','product_t3_accessories'))
logging.info (f'accesories transformation collection_FE_tag ended')
#--accesories--#

#--testing cosin csv--#
# accesories_df=pd.read_csv('D:/PROJECTS/Recomendation_System_FP/cosin_accessories_df.csv')
# clothing_df=pd.read_csv('D:/PROJECTS/Recomendation_System_FP/cosin_clothing_df.csv')
#--testing cosin csv--#        
#--cosin csv--#


logging.info (f"cosin stated {configur.get('product_transformation','product_t1_clothing')}")
dump_csv(
    combine_cosin_df(
                    # accesories_df,clothing_df,
                    model_train(
                     load_csv(spark_session,
                     transformation_csv_path,configur.get('product_transformation','product_t1_clothing')),
                     model_artifact(model_artifact_df,
                     configur.get('model','model_version'))    ),

                     
                    model_train(
                    load_csv(spark_session,
                    transformation_csv_path,configur.get('product_transformation','product_t1_accessories')),
                    model_artifact(model_artifact_df,
                    configur.get('model','model_version'))    ),
                    Product_tag_df,Tag_cloud_df,Product_df,Collection_Category_tag_df,feature_accesories,feature_clothing
                    ),
                     
                     file_path=cosin_path,
                     table_name=configur.get('product_transformation_path','product_t1')
                    )


logging.info (f"cosin ended {configur.get('product_transformation','product_t1_clothing')}")


logging.info (f"cosin started {configur.get('product_transformation','product_t2_clothing')}")
dump_csv(
    combine_cosin_df(
                    #accesories_df,clothing_df,
                    model_train(
                     load_csv(spark_session,
                     transformation_csv_path,configur.get('product_transformation','product_t2_clothing')),
                     model_artifact(model_artifact_df,
                     configur.get('model','model_version'))    ),

                     
                    model_train(
                    load_csv(spark_session,
                    transformation_csv_path,configur.get('product_transformation','product_t2_accessories')),
                    model_artifact(model_artifact_df,
                    configur.get('model','model_version'))    ),
                    Product_tag_df,Tag_cloud_df,Product_df,Collection_Category_tag_df,feature_accesories,feature_clothing
                     ),
                     
                     file_path=cosin_path,
                     table_name=configur.get('product_transformation_path','product_t2')
)

logging.info (f"cosin ended {configur.get('product_transformation','product_t2_clothing')}")
logging.info (f"cosin started {configur.get('product_transformation','product_t3_clothing')}")
dump_csv(
    combine_cosin_df(
                    #accesories_df,clothing_df,
                    model_train(
                     load_csv(spark_session,
                     transformation_csv_path,configur.get('product_transformation','product_t3_clothing')),
                     model_artifact(model_artifact_df,
                     configur.get('model','model_version'))    ),

                    model_train(
                    load_csv(spark_session,
                    transformation_csv_path,configur.get('product_transformation','product_t3_accessories')),
                    model_artifact(model_artifact_df,
                    configur.get('model','model_version'))    ),
                    Product_tag_df,Tag_cloud_df,Product_df,Collection_Category_tag_df,feature_accesories,feature_clothing
                     ),
                     
                     file_path=cosin_path,
                     table_name=configur.get('product_transformation_path','product_t3')
)

logging.info (f"cosin ended {configur.get('product_transformation','product_t3_clothing')}")


