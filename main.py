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

BASE_PATH=os.getenv('BASEPATH')
CONN_URL = os.getenv('CONN_URL')
PASSWORD = os.getenv('PASSWORD')
USERNAME=os.getenv('USER_NAME')
configur = ConfigParser() 
configur.read(BASE_PATH+'config.ini')
spark_session=session(spark_driver=configur.get('driver_path_pyspark','pyspark'))



print('product')
dump_csv(product_transformation
        (
        extract_data(spark=spark_session,table_name=configur.get('table_extract','product'),
        password=PASSWORD,user_name=USERNAME,fetech_size=configur.get('size','fetch_size'),conn_url=CONN_URL)
        ),file_path=configur.get('BasePath','Path'),table_name=configur.get('table_extract','product')
        )


dump_csv(producttag_transformation
        (
        extract_data(spark=spark_session,table_name=configur.get('table_extract','product_tag'),
        password=PASSWORD,user_name=USERNAME,fetech_size=configur.get('size','fetch_size'),
        conn_url=CONN_URL)
        ),file_path=configur.get('BasePath','Path'),table_name=configur.get('table_extract','product_tag')
        )
print('producttag')
dump_csv(collection_category_transformation
        (extract_data(spark=spark_session,table_name=configur.get('table_extract','collection_category'),
                 password=PASSWORD,user_name=USERNAME,fetech_size=configur.get('size','fetch_size'),conn_url=CONN_URL)
        ),file_path=configur.get('BasePath','Path'),table_name=configur.get('table_extract','collection_category'))

print('collection_category_tag')
dump_csv(collection_category_tag_transformation
        (extract_data(spark=spark_session,table_name=configur.get('table_extract','collection_category_tag'),
                 password=PASSWORD,user_name=USERNAME,fetech_size=configur.get('size','fetch_size'),conn_url=CONN_URL)
        ),file_path=configur.get('BasePath','Path'),table_name=configur.get('table_extract','collection_category_tag'))

print('tag_cloud')
dump_csv(tag_cloud_transformation
         (extract_data(spark=spark_session,table_name=configur.get('table_extract','tag_cloud'),
                 password=PASSWORD,user_name=USERNAME,fetech_size=configur.get('size','fetch_size'),conn_url=CONN_URL)
         ),file_path=configur.get('BasePath','Path'),table_name=configur.get('table_extract','tag_cloud'))