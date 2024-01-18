
# import boto3
# Replace these values with your own
# bucket_name = 'our-picks-for-you-section'
# csv_file_path = 'D:/PROJECTS/Recomendation_System_FP/cosin_simmilarity2.csv'
# s3_key = 'csv/cosin_simmilarity_v1.csv'

# # Create an S3 client
# s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

# # Upload the file
# s3.upload_file(csv_file_path, bucket_name, s3_key)

# print(f'{csv_file_path} has been uploaded to {bucket_name}/{s3_key}')


import requests

url = 'https://dev-api.fashionpass.com/api/v1/Content/UploadFiles'
files = {'file': ('validated_cosin_simmilarity.csv', open('D:/PROJECTS/Recomendation_System_FP/Code/notebook/validated_cosin_simmilarity.csv', 'rb'))}
data = {'folder': 'ml_csv'}

response = requests.post(url, files=files, data=data)

print(response.text)
{"success":true,"message":"","links":["https://fashionpass.s3.us-west-1.amazonaws.com/ml_csv/validated_cosin_simmilarity.csv"]}