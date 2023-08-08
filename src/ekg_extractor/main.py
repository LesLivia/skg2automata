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

    labels = ["Pizza"]
    entities = querier.get_entities_by_labels(labels)
    for e in entities[:5]:
        print(e)

    activities = querier.get_activities()
    for a in activities[:5]:
        print(a)

    events = querier.get_unique_events()
    for e in events:
        print(e)

    start_t = Timestamp(1970, 1, 1, 0, 0, 0)
    end_t = Timestamp(1970, 1, 2, 0, 0, 0)
    events = querier.get_events_by_date(start_t, end_t)
    for e in events[:5]:
        print(e.activity)

    conn.close_connection(driver)
    LOGGER.info('EKG querying done.')
except AuthError:
    LOGGER.error('Connection to DB could not be established.')
