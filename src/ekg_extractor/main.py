import src.ekg_extractor.mgrs.db_connector as conn
from src.ekg_extractor.logger.logger import Logger
from src.ekg_extractor.mgrs.ekg_queries import Ekg_Querier
from neo4j.exceptions import AuthError
from src.ekg_extractor.model.schema import Timestamp

LOGGER = Logger('main')

LOGGER.info('Starting...')

try:
    driver = conn.get_driver()
    querier = Ekg_Querier(driver)
    entities = querier.get_entities()
    for entity in entities[:5]:
        events = querier.get_events_by_entity(entity.extra_attr[querier.schema['event_properties']['en_id']])
        print(len(events))
    conn.close_connection(driver)
    LOGGER.info('EKG querying done.')
except AuthError:
    LOGGER.error('Connection to DB could not be established.')
