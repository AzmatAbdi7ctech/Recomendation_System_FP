from Code.utils.libraries import * 
from Code.data.dataprocessing.extract.extract import *
from Code.data.dataprocessing.load.load import *
from Code.data.dataprocessing.extract.product import *
from Code.data.dataprocessing.extract.product_tag import *
from Code.data.dataprocessing.extract.collection_category import *
from Code.data.dataprocessing.extract.collection_category_tag import *
from Code.data.dataprocessing.extract.tag_cloud  import *
from sessions import *
from pyspark.sql.functions import date_format
from Code.utils.logs import *
import os
print('My nested folder : ', os.getcwd())

confing_name='config.ini'
BASE_PATH=os.getcwd()
configur = ConfigParser() 
print(str(os.path.join(BASE_PATH,'Code',confing_name)).replace('\\', '/'))
configur.read(os.path.join(BASE_PATH,'Code',confing_name)  ) #for dev
CONN_URL = configur.get('credantials','CONN_URL')
PASSWORD = configur.get('credantials','PASSWORD')
USERNAME=configur.get('credantials','USER_NAME')
spark_session=session()
databse_csv_path=f"{BASE_PATH}/Code/Database/"

path=f'{BASE_PATH}/Code/logs/'
try:
    os.makedirs(path)
    print(path)
except:
        logging.info(f"FOLDER ALREADY EXIST{path}")

try:
    os.makedirs(databse_csv_path)
    print(databse_csv_path)
except:
        logging.info(f"FOLDER ALREADY EXIST{databse_csv_path}")

# spark_driver=configur.get('driver_path_pyspark','pyspark')
try:
        dump_csv(product_transformation
                (
                extract_data(spark=spark_session,table_name=configur.get('table_extract','product'),
                password=PASSWORD,user_name=USERNAME,fetech_size=configur.get('size','fetch_size'),conn_url=CONN_URL),
                column_list=['product_id','product_title','published_at'])
                ,file_path=databse_csv_path,table_name=configur.get('table_extract','product')
                )
        logging.info('--product table extracted--')
except:
        logging.debug('--product table not extracted--')


try:
        dump_csv(producttag_transformation
        (
        extract_data(spark=spark_session,table_name=configur.get('table_extract','product_tag'),
        password=PASSWORD,user_name=USERNAME,fetech_size=configur.get('size','fetch_size'),
        conn_url=CONN_URL),column_list=['product_id','tag_id']
        ),file_path=databse_csv_path,table_name=configur.get('table_extract','product_tag')
        )
        logging.info('--product tags table extracted--')
except:
        logging.debug('--product tags table not extracted--')


try:        
        dump_csv(collection_category_transformation
                (extract_data(spark=spark_session,table_name=configur.get('table_extract','collection_category'),
                        password=PASSWORD,user_name=USERNAME,fetech_size=configur.get('size','fetch_size'),conn_url=CONN_URL)
                ,column_list=['id','name','parent_id']),file_path=databse_csv_path,table_name=configur.get('table_extract','collection_category'))
        logging.info('--collection category table extracted--')
except: 
        logging.debug('--collection category table not extracted--')


try:
        dump_csv(collection_category_tag_transformation
                (extract_data(spark=spark_session,table_name=configur.get('table_extract','collection_category_tag'),
                         password=PASSWORD,user_name=USERNAME,fetech_size=configur.get('size','fetch_size'),conn_url=CONN_URL)
                ,column_list=['id','category_id','tag_id']),file_path=databse_csv_path,table_name=configur.get('table_extract','collection_category_tag'))
        logging.info('--collection category tag table--')        
except:
        logging.info('--collection category tag not extracted--')


try:
        dump_csv(tag_cloud_transformation
                 (extract_data(spark=spark_session,table_name=configur.get('table_extract','tag_cloud'),
                         password=PASSWORD,user_name=USERNAME,fetech_size=configur.get('size','fetch_size'),conn_url=CONN_URL),
                 column_list= ['id', 'tag_name']),file_path=databse_csv_path,table_name=configur.get('table_extract','tag_cloud'))
        logging.info('--tag_cloud table extracted--')
except:
        logging.info('--tag cloud table is not extracted--')