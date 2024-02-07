
from Code.utils.libraries import * 
import requests
from Code.utils.logs import *
from sessions import *
import boto3
confing_name='config.ini'
BASE_PATH=os.getcwd()
configur = ConfigParser() 
configur.read(os.path.join(BASE_PATH,'Code',confing_name)  ) #for dev
aws_access_key_id = configur.get('credantials','ACCESS_KEY')
aws_secret_access_key = configur.get('credantials','SECRECT_ACCESS_KEY')
bucket_name = 'our-picks-for-you-section'
cosin_path=str(os.path.join(BASE_PATH,'Code','cosin_similarity_registry')).replace('\\', '/')
val_path=f"{cosin_path}/{str(time.strftime('%Y_%m_%d'))}/"  
csv_file_path =f'{val_path}cosin_similarity_{datetime.date.today()}.csv' 
print(csv_file_path)
s3_key = f"cosin_csv/cosin_similarity_{datetime.date.today()}.csv"
s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

# Upload the file
url = 'https://dev-api.fashionpass.com/api/v1/Content/UploadFiles'
files = {'file': ('cosin_similarity.csv', open(csv_file_path,'rb'))}
data = {'folder': 'ml_csv'}
try:
    try:
        s3.upload_file(csv_file_path, bucket_name, s3_key)
        logging.info(f'{csv_file_path} uploaded {bucket_name}/{s3_key}')
    except:
        logging.info(f'{csv_file_path} has not been uploaded to {bucket_name}/{s3_key}')
    logging.info('file uploading to ml pick ')
    # try:
    #     response = requests.post(url, files=files, data=data)
    #     logging.info(f'--api--{response.text}')
    # except:
    #     logging.info('not upload through api')
except:
    logging.info('uploading file not found')
    
