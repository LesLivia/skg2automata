from neo4j.exceptions import AuthError

import src.ekg_extractor.mgrs.db_connector as conn
from src.ekg_extractor.logger.logger import Logger
from src.ekg_extractor.mgrs.ekg_queries import Ekg_Querier, SCHEMA
from src.ekg_extractor.model.schema import Timestamp
from src.ekg_extractor.model.semantics import EntityForest

LOGGER = Logger('main')

LOGGER.info('Starting...')

try:
    driver = conn.get_driver()
    querier = Ekg_Querier(driver)

    activities = querier.get_activities()
    print(','.join([a.act for a in activities]))

    if 'date' not in SCHEMA['event_properties']:
        start_t = 0
        end_t = 30000
    else:
        start_t = Timestamp(2023, 11, 4, 13, 0, 0)
        end_t = Timestamp(2023, 11, 4, 14, 30, 0)

    events = querier.get_events_by_date(start_t, end_t)
    for e in events:
        print(e.activity)

    resource = querier.get_resources(limit=1, random=True)[0]
    print(resource.entity_id, resource.extra_attr)

    entity_tree = querier.get_entity_tree(resource.entity_id, EntityForest([]))

    events = querier.get_events_by_entity_tree_and_timestamp(entity_tree.trees[0], start_t, end_t, pov='resource')
    for e in events:
        print(e.activity, e.timestamp)

    entity = querier.get_items(limit=1, random=True)[0]
    print(entity.entity_id, entity.extra_attr)

    entity_tree = querier.get_entity_tree(entity.entity_id, EntityForest([]), reverse=True)

    events = querier.get_events_by_entity_tree(entity_tree.trees[0], pov='item')
    for e in events:
        print(e.activity, e.timestamp)

    conn.close_connection(driver)
    LOGGER.info('EKG querying done.')
except AuthError:
    LOGGER.error('Connection to DB could not be established.')
