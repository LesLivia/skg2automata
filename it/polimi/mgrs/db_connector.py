import configparser

from neo4j import GraphDatabase, Driver

from it.polimi.logger.logger import Logger

LOGGER = Logger('DB Connector')

config = configparser.ConfigParser()
config.read('./resources/config/config.ini')
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
