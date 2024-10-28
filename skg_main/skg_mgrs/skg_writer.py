import configparser
import json
import os
from typing import List

from neo4j import Driver

from skg_main.skg_logger.logger import Logger
from skg_main.skg_model.automata import Automaton, Edge, Location
from skg_main.skg_model.schema import Activity, Entity

config = configparser.ConfigParser()
config.read(os.path.dirname(os.path.abspath(__file__)).split('skg_main')[0] + 'skg_main/resources/config/config.ini')
config.sections()

LOGGER = Logger('SKG Writer')


class Skg_Writer:
    def setup(self):
        LABELS_PATH = config['AUTOMATA TO SKG']['labels.path'].format(
            os.path.dirname(os.path.abspath(__file__)).split('skg_main')[0] + 'skg_main')
        LABELS = json.load(open(LABELS_PATH))

        NEO4J_CONFIG = config['NEO4J INSTANCE']['instance']

        if NEO4J_CONFIG.lower() == 'env_var':
            SCHEMA_NAME = os.environ['NEO4J_SCHEMA']
        else:
            SCHEMA_NAME = config['NEO4J SCHEMA']['schema.name']

        SCHEMA_PATH = config['NEO4J SCHEMA']['schema.path'].format(
            os.path.dirname(os.path.abspath(__file__)).split('skg_main')[0] + 'skg_main', SCHEMA_NAME)
        SCHEMA = json.load(open(SCHEMA_PATH))

        return LABELS, SCHEMA

    def __init__(self, driver: Driver):
        self.driver = driver
        self.LABELS, self.SCHEMA = self.setup()

    def get_sha_query_filter(self, automaton_name: str = None, pov=None, start=None, end=None, identifier='a'):
        if automaton_name is None and pov is None and start is None and end is None:
            return None
        else:
            query_filter = ""
            if automaton_name is not None:
                query_filter += "{}.{} = \"{}\"".format(identifier, self.LABELS['automaton_attr']['name'],
                                                        automaton_name)
            if pov is not None:
                if len(query_filter) > 0:
                    query_filter += " AND "
                query_filter += "{}.{} = \"{}\"".format(identifier, self.LABELS['automaton_attr']['pov'], pov)
            if start is not None:
                if len(query_filter) > 0:
                    query_filter += " AND "
                query_filter += "{}.{} = \"{}\"".format(identifier, self.LABELS['automaton_attr']['start'], start)
            if end is not None:
                if len(query_filter) > 0:
                    query_filter += " AND "
                query_filter += "{}.{} = \"{}\"".format(identifier, self.LABELS['automaton_attr']['end'], end)

            return query_filter

    def write_automaton(self, name: str = None, pov=None, start=None, end=None, path=None):
        AUTOMATON_PATH = config['AUTOMATA TO SKG']['automaton.path']

        if name is None:
            AUTOMATON_NAME = AUTOMATON_PATH.split('/')[-1].split('.')[0]
        else:
            AUTOMATON_NAME = name
            AUTOMATON_PATH = AUTOMATON_PATH.format(path, AUTOMATON_NAME)

        LOGGER.info('Loading {}...'.format(AUTOMATON_PATH))
        automaton = Automaton(name=AUTOMATON_NAME, filename=AUTOMATON_PATH)
        LOGGER.info('Found {} locations, {} edges.'.format(len(automaton.locations), len(automaton.edges)))

        AUTOMATON_QUERY = """
            CREATE (a:{} {{ {}: \"{}\", {}: \"{}\", {}: \"{}\", {}: \"{}\" }})
            RETURN elementId(a)
        """.format(self.LABELS['automaton_label'], self.LABELS['automaton_attr']['name'], AUTOMATON_NAME,
                   self.LABELS['automaton_attr']['pov'], pov, self.LABELS['automaton_attr']['start'], start,
                   self.LABELS['automaton_attr']['end'], end)
        with self.driver.session() as session:
            result = session.run(AUTOMATON_QUERY)
            new_automaton_id = [r['elementId(a)'] for r in result.data()][0]
        LOGGER.info("Created Automaton node.")

        LOCATION_QUERY = """
            MATCH (a: {})
            WHERE {}
            CREATE (l:{}:{} {{ {}: \"{}\" }}) -[:{}]-> (a)
        """
        for location in automaton.locations:
            query = LOCATION_QUERY.format(self.LABELS['automaton_label'],
                                          self.get_sha_query_filter(AUTOMATON_NAME, pov, start, end),
                                          self.LABELS['location_label'], self.LABELS['automaton_feature'],
                                          self.LABELS['location_attr']['name'], location.name, self.LABELS['has'])
            with self.driver.session() as session:
                session.run(query)
        LOGGER.info("Created Location nodes.")

        EDGE_TO_LOC_QUERY = """
            MATCH (s:{}), (t:{}), (a:{})
            WHERE s.{} = \"{}\" AND t.{} = \"{}\" AND {}
            AND (s) -[:{}]-> (a) AND (t) -[:{}]-> (a)
            CREATE (s) -[:{}]-> (e:{}:{} {{ {}: \"{}\" }}) -[:{}]-> (t)
            CREATE (a) <-[:{}]- (e)
        """
        for edge in automaton.edges:
            query = EDGE_TO_LOC_QUERY.format(self.LABELS['location_label'], self.LABELS['location_label'],
                                             self.LABELS['automaton_label'],
                                             self.LABELS['location_attr']['name'], edge.source.name,
                                             self.LABELS['location_attr']['name'], edge.target.name,
                                             self.get_sha_query_filter(AUTOMATON_NAME, pov, start, end),
                                             self.LABELS['has'], self.LABELS['has'],
                                             self.LABELS['edge_to_source'], self.LABELS['edge_label'],
                                             self.LABELS['automaton_feature'], self.LABELS['edge_attr']['event'],
                                             edge.label, self.LABELS['edge_to_target'], self.LABELS['has'])
            with self.driver.session() as session:
                session.run(query)
        LOGGER.info("Created Edge nodes.")

        return automaton, new_automaton_id

    def cleanup_all(self):
        DELETE_QUERY = """
        MATCH (x: {})
        DETACH DELETE x
        """

        query = DELETE_QUERY.format(self.LABELS['automaton_label'])
        with self.driver.session() as session:
            session.run(query)
        LOGGER.info("Deleted all automata nodes.")

        query = DELETE_QUERY.format(self.LABELS['location_label'])
        with self.driver.session() as session:
            session.run(query)
        LOGGER.info("Deleted all location nodes.")

        query = DELETE_QUERY.format(self.LABELS['edge_label'])
        with self.driver.session() as session:
            session.run(query)
        LOGGER.info("Deleted all edge nodes.")

    def cleanup(self, automaton_name: str = None, pov=None, start=None, end=None):
        if automaton_name is None and pov is None and start is None and end is None:
            self.cleanup_all()
        else:
            DELETE_QUERY = """
            MATCH (s:{}) -[:{}]-> (a:{}) 
            WHERE {} 
            DETACH DELETE s
            """

            query = DELETE_QUERY.format(self.LABELS['automaton_feature'], self.LABELS['has'],
                                        self.LABELS['automaton_label'],
                                        self.get_sha_query_filter(automaton_name, pov, start, end))
            with self.driver.session() as session:
                session.run(query)
            LOGGER.info("Deleted {} features.".format(automaton_name))

            DELETE_QUERY = """
            MATCH (a: {})
            WHERE {}
            DETACH DELETE a
            """

            query = DELETE_QUERY.format(self.LABELS['automaton_label'],
                                        self.get_sha_query_filter(automaton_name, pov, start, end))
            with self.driver.session() as session:
                session.run(query)
            LOGGER.info("Deleted {}, {}, {}, {} node.".format(automaton_name, pov, start, end))

    def create_semantic_link(self, automaton: Automaton, name: str, pov=None, start=None, end=None,
                             edge: Edge = None, loc: Location = None, act: Activity = None, ent: Entity = None,
                             entity_labels: List[str] = None):
        if edge is not None:
            if act is not None:
                CREATE_QUERY = """
                MATCH (s:{}) -[:{}]-> (e:{}) -[:{}]-> (t:{}) -[:{}]-> (aut:{}), (a:{})
                WHERE s.{} = \"{}\" and e.{} = \"{}\" and t.{} = \"{}\" and {} and a.{} = \"{}\"
                CREATE (e) -[:{}]-> (a) 
                """
                query = CREATE_QUERY.format(self.LABELS['location_label'], self.LABELS['edge_to_source'],
                                            self.LABELS['edge_label'], self.LABELS['edge_to_target'],
                                            self.LABELS['location_label'], self.LABELS['has'],
                                            self.LABELS['automaton_label'], self.SCHEMA['activity'],
                                            self.LABELS['location_attr']['name'], edge.source.name,
                                            self.LABELS['edge_attr']['event'], edge.label,
                                            self.LABELS['location_attr']['name'], edge.target.name,
                                            self.get_sha_query_filter(automaton.name, pov, start, end, 'aut'),
                                            self.SCHEMA['activity_properties']['id'][0], act.act, name)
            elif ent is not None:
                if self.SCHEMA['entity_properties']['id'] == 'ID':
                    ent_filter = "ID(ent) = {}".format(ent.entity_id)
                else:
                    ent_filter = "ent.{} = \"{}\"".format(self.SCHEMA['entity_properties']['id'], ent.entity_id)

                CREATE_QUERY = """
                MATCH (s:{}) -[:{}]-> (e:{}) -[:{}]-> (t:{}) -[:{}]-> (aut:{}), (ent:{})
                WHERE s.{} = \"{}\" and e.{} = \"{}\" and t.{} = \"{}\" and {} and {}
                CREATE (e) -[:{}]-> (ent) 
                """
                query = CREATE_QUERY.format(self.LABELS['location_label'], self.LABELS['edge_to_source'],
                                            self.LABELS['edge_label'], self.LABELS['edge_to_target'],
                                            self.LABELS['location_label'], self.LABELS['has'],
                                            self.LABELS['automaton_label'], ':'.join(entity_labels),
                                            self.LABELS['location_attr']['name'], edge.source.name,
                                            self.LABELS['edge_attr']['event'], edge.label,
                                            self.LABELS['location_attr']['name'], edge.target.name,
                                            self.get_sha_query_filter(automaton.name, pov, start, end, 'aut'),
                                            ent_filter, name)
        elif loc is not None:
            CREATE_QUERY = """
            MATCH (l:{}) -[:{}]-> (aut:{}), (ent:{})
            WHERE l.{} = \"{}\" and {} and ent.{} = \"{}\"
            CREATE (l) -[:{}]-> (ent) 
            """
            query = CREATE_QUERY.format(self.LABELS['location_label'], self.LABELS['has'],
                                        self.LABELS['automaton_label'], ':'.join(entity_labels),
                                        self.LABELS['location_attr']['name'], loc.name,
                                        self.get_sha_query_filter(automaton.name, pov, start, end, 'aut'),
                                        self.SCHEMA['entity_properties']['id'], ent.entity_id, name)

        with self.driver.session() as session:
            session.run(query)
