import configparser
import os

from neo4j import GraphDatabase, Driver

from skg_main.skg_logger.logger import Logger

LOGGER = Logger('DB Connector')

config = configparser.ConfigParser()
config.read('{}/config/config.ini'.format(os.environ['SKG_RES_PATH']))
config.sections()

NEO4J_CONFIG = config['NEO4J INSTANCE']['instance']

if NEO4J_CONFIG.lower() == 'env_var':
    DB_URI = os.environ['NEO4J_URI']
    DB_USER = os.environ['NEO4J_USERNAME']
    DB_PW = os.environ['NEO4J_PASSWORD']
else:
    config.read('{}/config/{}.ini'.format(os.environ['SKG_RES_PATH'], NEO4J_CONFIG))
    config.sections()

    DB_SCHEME = config['NEO4J SETTINGS']['db.scheme']
    DB_IP = config['NEO4J SETTINGS']['db.ip']
    DB_PORT = config['NEO4J SETTINGS']['db.port']
    DB_USER = config['NEO4J SETTINGS']['db.user']
    DB_ENCRIPTION = config['NEO4J SETTINGS']['db.encryption']
    if DB_ENCRIPTION == 'x':
        DB_URI = '{}://{}:{}'.format(DB_SCHEME, DB_IP, DB_PORT)
    else:
        DB_URI = '{}+{}://{}:{}'.format(DB_SCHEME, DB_ENCRIPTION, DB_IP, DB_PORT)
    DB_PW = config['NEO4J SETTINGS']['db.password']


def get_driver():
    LOGGER.debug('Setting up connection to NEO4J DB...')
    driver = GraphDatabase.driver(DB_URI, auth=(DB_USER, DB_PW))
    return driver


def close_connection(driver: Driver):
    driver.close()
