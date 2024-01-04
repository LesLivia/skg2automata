import configparser
import json
import os

from neo4j import Driver
from src.skg2automata.logger.logger import Logger

config = configparser.ConfigParser()
if 'submodules' in os.listdir():
    curr_path = os.getcwd() + '/submodules/skg2automata'
else:
    curr_path = os.getcwd().split('src/skg2automata')[0]
config.read('{}/resources/config/config.ini'.format(curr_path))
config.sections()

SCHEMA_NAME = config['NEO4J SCHEMA']['schema.name']
SCHEMA_PATH = config['NEO4J SCHEMA']['schema.path'].format(curr_path, SCHEMA_NAME)
SCHEMA = json.load(open(SCHEMA_PATH))

LOGGER = Logger('SKG Writer')

class Skg_Writer:
    def __init__(self, driver: Driver):
        self.driver = driver
