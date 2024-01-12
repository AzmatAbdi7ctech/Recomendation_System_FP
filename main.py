from Code.utils.libraries import * 
from Code.data.dataprocessing.extract.extract import *
from Code.data.dataprocessing.load.load import *
from sessions import *

BASE_PATH=os.getenv('BASEPATH')
CONN_URL = os.getenv('CONN_URL')
PASSWORD = os.getenv('PASSWORD')
USERNAME=os.getenv('USER_NAME')
configur = ConfigParser() 
configur.read(BASE_PATH+'config.ini')
spark_path=configur.get('driver_path_pyspark','pyspark')
spark_session=session(spark_driver=spark_path)


df=extract_data(spark=spark_session,table_name=configur.get('table_extract','product'),password=PASSWORD,user_name=USERNAME,fetech_size=configur.get('size','fetch_size'),conn_url=CONN_URL)
dump_csv(df,file_path=configur.get('BasePath','Path'),table_name=configur.get('table_extract','product'))
# sys.exit()
