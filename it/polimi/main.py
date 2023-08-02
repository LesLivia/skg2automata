import it.polimi.mgrs.db_connector as conn
from it.polimi.logger.logger import Logger
from it.polimi.mgrs.ekg_queries import Ekg_Querier
from neo4j.exceptions import AuthError
from it.polimi.model.schema import Timestamp

LOGGER = Logger('main')

LOGGER.info('Starting...')

try:
    driver = conn.get_driver()
    querier = Ekg_Querier(driver)
    start_t = Timestamp(1970, 1, 2, 0, 0, 0)
    end_t = Timestamp(1970, 1, 3, 0, 0, 0)
    events = querier.get_events_by_date(end_t=start_t)
    print(events[0])
    unique_events = querier.get_unique_events()
    print(unique_events[0])
    entities = querier.get_entities()
    print(entities[0])
    sensors = querier.get_sensors()
    print(sensors[0])
    conn.close_connection(driver)
    LOGGER.info('EKG querying done.')
except AuthError:
    LOGGER.error('Connection to DB could not be established.')
