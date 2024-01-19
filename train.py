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
Features_df=load_csv(spark_session,configur.get('feature_list','feature_list_path'))
Product_df=load_csv(spark_session,configur.get('dataframe','product'))
Collection_Category_df=load_csv(spark_session,configur.get('dataframe','collection_category'))
Collection_Category_tag_df=load_csv(spark_session,configur.get('dataframe','collection_category_tag'))
Product_tag_df=load_csv(spark_session,configur.get('dataframe','product_tag'))
Tag_cloud_df=load_csv(spark_session,configur.get('dataframe','tag_cloud'))
feature=feature_list(Features_df)
model_artifact_df=load_csv(spark_session,configur.get('model','model_artifact_path'))



dump_csv(tag_cloud_join_df(Collection_Category_df,Collection_Category_tag_df,
                           Tag_cloud_df,Product_tag_df,feature,'left'),
                           file_path=configur.get('BasePath','Path'),
                           table_name=configur.get('product_transformation','product_t1'))
logging.info('tag transformation done')

dump_csv(collection_tag_join_df(Collection_Category_df,Collection_Category_tag_df,
                           Tag_cloud_df,Product_tag_df,feature,'left'),
                           file_path=configur.get('BasePath','Path'),
                           table_name=configur.get('product_transformation','product_t2'))
logging.info('collection tag transfomation done')
dump_csv(collection_FE_tag_join_df(Collection_Category_df,Collection_Category_tag_df,
                           Tag_cloud_df,Product_tag_df,feature,'left'),
                           file_path=configur.get('BasePath','Path'),
                           table_name=configur.get('product_transformation','product_t3'))

logging.info('collection_tag_FE transformation done')
dump_csv(model_train(
                     load_csv(spark_session,
                     configur.get('product_transformation_path','product_t1')),
                     model_artifact(model_artifact_df,
                     configur.get('model','model_version'))    ),
                     file_path=configur.get('cosin_similarity_base_path','path'),
                     table_name=configur.get('product_transformation','product_t1'))


logging.info(f"cosing similarity_genrated {configur.get('product_transformation','product_t1')}")
dump_csv(model_train(load_csv(spark_session,
                     configur.get('product_transformation_path','product_t2')),
                     model_artifact(model_artifact_df,
                     configur.get('model','model_version'))),
                     file_path=configur.get('cosin_similarity_base_path','path'),
                     table_name=configur.get('product_transformation','product_t2'))

logging.info(f"cosing similarity_genrated {configur.get('product_transformation','product_t2')}")
dump_csv(model_train(load_csv(spark_session,
                     configur.get('product_transformation_path','product_t3')),
                     model_artifact(model_artifact_df,
                     configur.get('model','model_version'))    ),
                     file_path=configur.get('cosin_similarity_base_path','path'),
                     table_name=configur.get('product_transformation','product_t3'))

logging.info(f"cosing similarity_genrated {configur.get('product_transformation','product_t3')}")