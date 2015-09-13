import os
import logging
import json

logFormatter = logging.Formatter('%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s')
LOGGER = logging.getLogger('ugly_duckling')
LOG_DIRECTORY = os.getcwd()
fileHandler = logging.FileHandler(LOG_DIRECTORY + "/ugly_duckling.log")
fileHandler.setFormatter(logFormatter)
LOGGER.addHandler(fileHandler)
consoleHandler = logging.StreamHandler()
consoleHandler.setFormatter(logFormatter)
LOGGER.addHandler(consoleHandler)
LOGGER.setLevel('DEBUG')


with open('config.json') as login_data:
    login_data = json.load(login_data)
