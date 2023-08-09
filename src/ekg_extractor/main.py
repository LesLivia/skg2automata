from neo4j.exceptions import AuthError

import src.ekg_extractor.mgrs.db_connector as conn
from src.ekg_extractor.logger.logger import Logger
from src.ekg_extractor.mgrs.ekg_queries import Ekg_Querier

LOGGER = Logger('main')

LOGGER.info('Starting...')

try:
    driver = conn.get_driver()
    querier = Ekg_Querier(driver)

    entity_label_hierarchy = querier.get_entity_labels_hierarchy()
    print(entity_label_hierarchy)

    activities = querier.get_activities()
    for a in activities[:5]:
        print(a)

    events = querier.get_events_by_entity('B_1000_000001')
    for e in events:
        print(e.activity)

    conn.close_connection(driver)
    LOGGER.info('EKG querying done.')
except AuthError:
    LOGGER.error('Connection to DB could not be established.')
