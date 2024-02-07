import logging
import os
BASE_PATH=os.getcwd()
BASE_PATH=str(os.path.join(BASE_PATH).replace('\\', '/'))
print(BASE_PATH)
logging.basicConfig(filename=f'{BASE_PATH}/Code/logs/app.log',
                    format='%(asctime)s %(message)s',
                    filemode='a')
logger = logging.getLogger()
logger.setLevel(logging.INFO)