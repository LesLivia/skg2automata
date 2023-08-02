import configparser
import os

from neo4j import GraphDatabase, Driver

from src.ekg_extractor.logger.logger import Logger

LOGGER = Logger('DB Connector')

config = configparser.ConfigParser()
if 'submodules' in os.listdir():
    curr_path = os.getcwd() + '/submodules/ekg_extractor'
else:
    curr_path = os.getcwd().split('src/ekg_extractor')[0]
config.read('{}/resources/config/config.ini'.format(curr_path))
config.sections()

DB_IP = config['NEO4J SETTINGS']['db.ip']
DB_PORT = config['NEO4J SETTINGS']['db.port']
DB_URI = 'bolt://{}:{}'.format(DB_IP, DB_PORT)
DB_PW = config['NEO4J SETTINGS']['db.password']


def get_driver():
    LOGGER.info('Setting up connection to NEO4J DB...')
    driver = GraphDatabase.driver(DB_URI, auth=('neo4j', DB_PW))
    return driver


def close_connection(driver: Driver):
    driver.close()
