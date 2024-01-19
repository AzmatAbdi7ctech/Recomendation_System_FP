
from Code.utils.libraries import * 
import requests
from Code.utils.logs import *
from sessions import *
BASE_PATH=os.getenv('BASEPATH')
CONN_URL = os.getenv('CONN_URL')
PASSWORD = os.getenv('PASSWORD')
USERNAME=os.getenv('USER_NAME')
configur = ConfigParser() 
configur.read(BASE_PATH+'config.ini')
import boto3
BASE_PATH=os.getenv('BASEPATH')
configur = ConfigParser() 
configur.read(BASE_PATH+'config.ini')
aws_access_key_id = os.getenv('ACCESS_KEY')
aws_secret_access_key = os.getenv('SECRECT_ACCESS_KEY')
bucket_name = 'our-picks-for-you-section'
csv_file_path = f"{configur.get('cosin_similarity_base_path','path')}cosin_similarity_{datetime.date.today()}.csv"
s3_key = f"cosin_csv/cosin_similarity_{datetime.date.today()}.csv"
s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
# Upload the file
url = 'https://dev-api.fashionpass.com/api/v1/Content/UploadFiles'
files = {'file': ('cosin_similarity.csv', open(csv_file_path,'rb'))}#f"{configur.get('cosin_similarity_base_path','path')}cosin_similarity_{datetime.date.today()}.csv", 'rb'))}
data = {'folder': 'ml_csv'}
try:
    try:
        s3.upload_file(csv_file_path, bucket_name, s3_key)
    except:
        logging.info(f'{csv_file_path} has not been uploaded to {bucket_name}/{s3_key}')
    logging.info('file uploaded to ml pick ')
    try:
        response = requests.post(url, files=files, data=data)
        logging.info(f'--api--{response.text}')
    except:
        logging.info('not upload through api')
except:
    logging.info('uploading file not found')
    
