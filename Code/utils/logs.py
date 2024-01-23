import logging
logging.basicConfig(filename="D:/PROJECTS/Recomendation_System_FP/Code/logs/app.log",
                    format='%(asctime)s %(message)s',
                    filemode='a')
logger = logging.getLogger()
logger.setLevel(logging.INFO)