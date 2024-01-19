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
BASE_PATH=os.getenv('BASEPATH')
CONN_URL = os.getenv('CONN_URL')
PASSWORD = os.getenv('PASSWORD')
USERNAME=os.getenv('USER_NAME')
configur = ConfigParser() 
configur.read(BASE_PATH+'config.ini')
spark_session=session(spark_driver=configur.get('driver_path_pyspark','pyspark'))


dump_csv(product_transformation
        (
        extract_data(spark=spark_session,table_name=configur.get('table_extract','product'),
        password=PASSWORD,user_name=USERNAME,fetech_size=configur.get('size','fetch_size'),conn_url=CONN_URL)
        ),file_path=configur.get('BasePath','Path'),table_name=configur.get('table_extract','product')
        )

logging.info('--product table extracted--')
dump_csv(producttag_transformation
        (
        extract_data(spark=spark_session,table_name=configur.get('table_extract','product_tag'),
        password=PASSWORD,user_name=USERNAME,fetech_size=configur.get('size','fetch_size'),
        conn_url=CONN_URL)
        ),file_path=configur.get('BasePath','Path'),table_name=configur.get('table_extract','product_tag')
        )
logging.info('--product_tag table extracted--')
dump_csv(collection_category_transformation
        (extract_data(spark=spark_session,table_name=configur.get('table_extract','collection_category'),
                 password=PASSWORD,user_name=USERNAME,fetech_size=configur.get('size','fetch_size'),conn_url=CONN_URL)
        ),file_path=configur.get('BasePath','Path'),table_name=configur.get('table_extract','collection_category'))

logging.info('--collection_category table extracted--')

dump_csv(collection_category_tag_transformation
        (extract_data(spark=spark_session,table_name=configur.get('table_extract','collection_category_tag'),
                 password=PASSWORD,user_name=USERNAME,fetech_size=configur.get('size','fetch_size'),conn_url=CONN_URL)
        ),file_path=configur.get('BasePath','Path'),table_name=configur.get('table_extract','collection_category_tag'))

logging.info('--collection_category_tag table extracted--')

dump_csv(tag_cloud_transformation
         (extract_data(spark=spark_session,table_name=configur.get('table_extract','tag_cloud'),
                 password=PASSWORD,user_name=USERNAME,fetech_size=configur.get('size','fetch_size'),conn_url=CONN_URL)
         ),file_path=configur.get('BasePath','Path'),table_name=configur.get('table_extract','tag_cloud'))

logging.info('--tag cloud table extracted--')