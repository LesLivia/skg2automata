import configparser
import os

from neo4j import GraphDatabase, Driver

from src.skg2automata.logger.logger import Logger

LOGGER = Logger('DB Connector')

config = configparser.ConfigParser()
if 'submodules' in os.listdir():
    curr_path = os.getcwd() + '/submodules/skg2automata'
else:
    curr_path = os.getcwd().split('src/skg2automata')[0]

config.read('{}/resources/config/config.ini'.format(curr_path))
config.sections()

NEO4J_CONFIG = config['NEO4J INSTANCE']['instance']

config.read('{}/resources/config/{}.ini'.format(curr_path, NEO4J_CONFIG))
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
    LOGGER.info('Setting up connection to NEO4J DB...')
    driver = GraphDatabase.driver(DB_URI, auth=(DB_USER, DB_PW))
    return driver


def close_connection(driver: Driver):
    driver.close()
