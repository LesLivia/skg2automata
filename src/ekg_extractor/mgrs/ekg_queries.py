import configparser
import json
import os
from typing import List, Tuple

from neo4j import Driver, Result
from tqdm import tqdm

from src.ekg_extractor.model.schema import Event, Entity, Activity
from src.ekg_extractor.model.semantics import EntityHierarchy

config = configparser.ConfigParser()
if 'submodules' in os.listdir():
    curr_path = os.getcwd() + '/submodules/ekg_extractor'
else:
    curr_path = os.getcwd().split('src/ekg_extractor')[0]
config.read('{}/resources/config/config.ini'.format(curr_path))
config.sections()

SCHEMA_PATH = config['NEO4J SCHEMA']['schema.path'].format(curr_path, config['NEO4J SCHEMA']['schema.name'])
SCHEMA = json.load(open(SCHEMA_PATH))


class Ekg_Querier:
    def __init__(self, driver: Driver):
        self.driver = driver

    def get_events(self):
        with self.driver.session() as session:
            events_recs: Result = session.run("MATCH (e:{}) RETURN e".format(SCHEMA['event']))
            return [Event.parse_evt(e, SCHEMA['event_properties']) for e in
                    tqdm(events_recs.data())]

    def get_unique_events(self):
        all_events = self.get_events()
        unique_events = [e.activity for e in all_events]
        return set(unique_events)

    def get_events_by_timestamp(self, start_t: int = None, end_t: int = None):
        query = ""

        if start_t is not None and end_t is None:
            query = "MATCH (e:{}) WHERE e.{} > {} RETURN e " \
                    "ORDER BY e.{}".format(SCHEMA['event'], SCHEMA['event_properties']['timestamp'],
                                           str(start_t), SCHEMA['event_properties']['timestamp'])
        elif start_t is None and end_t is not None:
            query = "MATCH (e:{}) WHERE e.{} < {} RETURN e " \
                    "ORDER BY e.{}".format(SCHEMA['event'], SCHEMA['event_properties']['timestamp'],
                                           str(end_t), SCHEMA['event_properties']['timestamp'])
        elif start_t is not None and end_t is not None:
            query = "MATCH (e:{}) where e.{} > {} and e.{} < {} return e " \
                    "ORDER BY e.{}".format(SCHEMA['event'], SCHEMA['event_properties']['timestamp'],
                                           str(start_t), SCHEMA['event_properties']['timestamp'],
                                           str(end_t), SCHEMA['event_properties']['timestamp'])
        with self.driver.session() as session:
            events_recs: Result = session.run(query)
            return [Event.parse_evt(e, SCHEMA['event_properties']) for e in
                    tqdm(events_recs.data())]

    def get_events_by_date(self, start_t=None, end_t=None):
        query = ""

        if start_t is None and end_t is None:
            return self.get_events()

        if 'date' not in SCHEMA['event_properties']:
            return self.get_events_by_timestamp(start_t, end_t)
        elif start_t is not None and end_t is None:
            query = "MATCH (e:{}) WHERE e.{} > datetime({{epochmillis: apoc.date.parse(\"{}\", " \
                    "\"ms\", \"yyyy-MM-dd hh:mm:ss\")}}) RETURN e " \
                    "ORDER BY e.{}".format(SCHEMA['event'], SCHEMA['event_properties']['date'], str(start_t),
                                           SCHEMA['event_properties']['date'])
        elif start_t is None and end_t is not None:
            query = "MATCH (e:{}) WHERE e.{} < datetime({{epochmillis: apoc.date.parse(\"{}\", " \
                    "\"ms\", \"yyyy-MM-dd hh:mm:ss\")}}) RETURN e " \
                    "ORDER BY e.{}".format(SCHEMA['event'], SCHEMA['event_properties']['date'], str(end_t),
                                           SCHEMA['event_properties']['date'])
        elif start_t is not None and end_t is not None:
            query = "MATCH (e:{}) where e.{} > datetime({{epochmillis: apoc.date.parse(\"{}\", " \
                    "\"ms\", \"yyyy-MM-dd hh:mm:ss\")}}) and e.{} < datetime({{epochmillis: apoc.date.parse(\"{}\", " \
                    "\"ms\", \"yyyy-MM-dd hh:mm:ss\")}}) return e " \
                    "ORDER BY e.{}".format(SCHEMA['event'], SCHEMA['event_properties']['date'], str(start_t),
                                           SCHEMA['event_properties']['date'], str(end_t),
                                           SCHEMA['event_properties']['date'])
        with self.driver.session() as session:
            events_recs: Result = session.run(query)
            return [Event.parse_evt(e, SCHEMA['event_properties']) for e in
                    tqdm(events_recs.data())]

    def get_events_by_entity(self, en_id: str):
        query = "MATCH (e:{}) - [:{}] - (y:{}) WHERE toString(y.{}) = \"{}\" RETURN e " \
                "ORDER BY e.{}".format(SCHEMA['event'], SCHEMA['event_to_entity'], SCHEMA['entity'],
                                       SCHEMA['entity_properties']['id'], en_id,
                                       SCHEMA['event_properties']['timestamp'])
        with self.driver.session() as session:
            events_recs: Result = session.run(query)
            return [Event.parse_evt(e, SCHEMA['event_properties']) for e in
                    tqdm(events_recs.data())]

    def get_entities(self):
        with self.driver.session() as session:
            entities = session.run("MATCH (e:{}) RETURN e".format(SCHEMA['entity']))
            return [Entity.parse_ent(e, SCHEMA['entity_properties']) for e in tqdm(entities.data())]

    def get_entities_by_labels(self, labels: List[str] = None):
        if labels is None:
            return self.get_entities()
        else:
            query_filter = "WHERE " + ' and '.join(["e:{}".format(l) for l in labels])

        query = "MATCH (e:{}) {} RETURN e".format(SCHEMA['entity'], query_filter)
        with self.driver.session() as session:
            entities = session.run(query)
            return [Entity.parse_ent(e, SCHEMA['entity_properties']) for e in tqdm(entities.data())]

    def get_entity_labels_tree(self):
        if 'entity_to_entity' not in SCHEMA:
            return [[l] for l in SCHEMA['entity_labels']]

        query = "MATCH (e1:{}) - [:{}] -> (e2:{}) RETURN labels(e1), labels(e2)".format(SCHEMA['entity'],
                                                                                        SCHEMA['entity_to_entity'],
                                                                                        SCHEMA['entity'])
        with self.driver.session() as session:
            results = session.run(query)

            rels: List[Tuple[str, str]] = []
            for res in results.data():
                rels.append(('-'.join([r for r in res['labels(e1)'] if r != SCHEMA['entity']]),
                             '-'.join([r for r in res['labels(e2)'] if r != SCHEMA['entity']])))

            return EntityHierarchy.get_labels_tree(set(rels))

    def get_activities(self):
        with self.driver.session() as session:
            activities = session.run("MATCH (s:{}) RETURN s".format(SCHEMA['activity']))
            return [Activity.parse_act(s, SCHEMA['activity_properties']) for s in tqdm(activities.data())]
