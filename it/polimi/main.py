import it.polimi.mgrs.db_connector as conn
from it.polimi.logger.logger import Logger
from it.polimi.mgrs.ekg_queries import Ekg_Querier
from neo4j.exceptions import AuthError

LOGGER = Logger('main')

LOGGER.info('Starting...')

try:
    driver = conn.get_driver()
    querier = Ekg_Querier(driver)
    print(querier.get_events()[0])
    conn.close_connection(driver)
    LOGGER.info('EKG querying done.')
except AuthError:
    LOGGER.error('Connection to DB could not be established.')
