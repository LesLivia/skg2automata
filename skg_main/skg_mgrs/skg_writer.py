import configparser
import json
import os
from typing import List

from neo4j import Driver

from skg_main.skg_logger.logger import Logger
from skg_main.skg_model.automata import Automaton, Edge, Location
from skg_main.skg_model.schema import Activity, Entity

config = configparser.ConfigParser()
config.read('{}/config/config.ini'.format(os.environ['SKG_RES_PATH']))
config.sections()

LABELS_PATH = config['AUTOMATA TO SKG']['labels.path'].format(os.environ['SKG_RES_PATH'])
LABELS = json.load(open(LABELS_PATH))

SCHEMA_NAME = config['NEO4J SCHEMA']['schema.name']
SCHEMA_PATH = config['NEO4J SCHEMA']['schema.path'].format(os.environ['SKG_RES_PATH'], SCHEMA_NAME)
SCHEMA = json.load(open(SCHEMA_PATH))

LOGGER = Logger('SKG Writer')


class Skg_Writer:
    def __init__(self, driver: Driver):
        self.driver = driver

    def write_automaton(self, name: str = None):
        AUTOMATON_PATH = config['AUTOMATA TO SKG']['automaton.path']

        if name is None:
            AUTOMATON_NAME = AUTOMATON_PATH.split('/')[-1].split('.')[0]
        else:
            AUTOMATON_NAME = name
            AUTOMATON_PATH = AUTOMATON_PATH.format(os.environ['RES_PATH'], AUTOMATON_NAME)

        LOGGER.info('Loading {}...'.format(AUTOMATON_PATH))
        automaton = Automaton(name=AUTOMATON_NAME, filename=AUTOMATON_PATH)
        LOGGER.info('Found {} locations, {} edges.'.format(len(automaton.locations), len(automaton.edges)))

        AUTOMATON_QUERY = """
            CREATE (a:{} {{ {}: \"{}\" }})
        """.format(LABELS['automaton_label'], LABELS['automaton_attr']['name'], AUTOMATON_NAME)
        with self.driver.session() as session:
            session.run(AUTOMATON_QUERY)
        LOGGER.info("Created Automaton node.")

        LOCATION_QUERY = """
            MATCH (a: {})
            WHERE a.{} = \"{}\"
            CREATE (l:{}:{} {{ {}: \"{}\" }}) -[:{}]-> (a)
        """
        for location in automaton.locations:
            query = LOCATION_QUERY.format(LABELS['automaton_label'], LABELS['automaton_attr']['name'],
                                          AUTOMATON_NAME, LABELS['location_label'], LABELS['automaton_feature'],
                                          LABELS['location_attr']['name'], location.name, LABELS['has'])
            with self.driver.session() as session:
                session.run(query)
        LOGGER.info("Created Location nodes.")

        EDGE_TO_LOC_QUERY = """
            MATCH (s:{}), (t:{}), (a:{})
            WHERE s.{} = \"{}\" AND t.{} = \"{}\" AND a.{} = \"{}\"
            AND (s) -[:{}]-> (a) AND (t) -[:{}]-> (a)
            CREATE (s) -[:{}]-> (e:{}:{} {{ {}: \"{}\" }}) -[:{}]-> (t)
            CREATE (a) <-[:{}]- (e)
        """
        for edge in automaton.edges:
            query = EDGE_TO_LOC_QUERY.format(LABELS['location_label'], LABELS['location_label'],
                                             LABELS['automaton_label'],
                                             LABELS['location_attr']['name'], edge.source.name,
                                             LABELS['location_attr']['name'], edge.target.name,
                                             LABELS['automaton_attr']['name'], AUTOMATON_NAME,
                                             LABELS['has'], LABELS['has'],
                                             LABELS['edge_to_source'], LABELS['edge_label'],
                                             LABELS['automaton_feature'], LABELS['edge_attr']['event'],
                                             edge.label, LABELS['edge_to_target'], LABELS['has'])
            with self.driver.session() as session:
                session.run(query)
        LOGGER.info("Created Edge nodes.")

        return automaton

    def cleanup_all(self):
        DELETE_QUERY = """
        MATCH (x: {})
        DETACH DELETE x
        """

        query = DELETE_QUERY.format(LABELS['automaton_label'])
        with self.driver.session() as session:
            session.run(query)
        LOGGER.info("Deleted all automata nodes.")

        query = DELETE_QUERY.format(LABELS['location_label'])
        with self.driver.session() as session:
            session.run(query)
        LOGGER.info("Deleted all location nodes.")

        query = DELETE_QUERY.format(LABELS['edge_label'])
        with self.driver.session() as session:
            session.run(query)
        LOGGER.info("Deleted all edge nodes.")

    def cleanup(self, automaton_name: str = None):
        if automaton_name is None:
            self.cleanup_all()
        else:
            DELETE_QUERY = """
            MATCH (s:{}) -[:{}]-> (a:{}) 
            WHERE a.{} = \"{}\"
            DETACH DELETE s
            """

            query = DELETE_QUERY.format(LABELS['automaton_feature'], LABELS['has'], LABELS['automaton_label'],
                                        LABELS['automaton_attr']['name'], automaton_name)
            with self.driver.session() as session:
                session.run(query)
            LOGGER.info("Deleted {} features.".format(automaton_name))

            DELETE_QUERY = """
            MATCH (a: {})
            WHERE a.{} = \"{}\"
            DETACH DELETE a
            """

            query = DELETE_QUERY.format(LABELS['automaton_label'],
                                        LABELS['automaton_attr']['name'], automaton_name)
            with self.driver.session() as session:
                session.run(query)
            LOGGER.info("Deleted {} node.".format(automaton_name))

    def create_semantic_link(self, automaton: Automaton, name: str, edge: Edge = None, loc: Location = None,
                             act: Activity = None, ent: Entity = None, entity_labels: List[str] = None):

        if edge is not None:
            if act is not None:
                CREATE_QUERY = """
                MATCH (s:{}) -[:{}]-> (e:{}) -[:{}]-> (t:{}) -[:{}]-> (aut:{}), (a:{})
                WHERE s.{} = \"{}\" and e.{} = \"{}\" and t.{} = \"{}\" and aut.{} = \"{}\" and a.{} = \"{}\"
                CREATE (e) -[:{}]-> (a) 
                """
                query = CREATE_QUERY.format(LABELS['location_label'], LABELS['edge_to_source'],
                                            LABELS['edge_label'], LABELS['edge_to_target'],
                                            LABELS['location_label'], LABELS['has'],
                                            LABELS['automaton_label'], SCHEMA['activity'],
                                            LABELS['location_attr']['name'], edge.source.name,
                                            LABELS['edge_attr']['event'], edge.label,
                                            LABELS['location_attr']['name'], edge.target.name,
                                            LABELS['automaton_attr']['name'], automaton.name,
                                            SCHEMA['activity_properties']['id'][0], act.act, name)
            elif ent is not None:
                CREATE_QUERY = """
                MATCH (s:{}) -[:{}]-> (e:{}) -[:{}]-> (t:{}) -[:{}]-> (aut:{}), (ent:{})
                WHERE s.{} = \"{}\" and e.{} = \"{}\" and t.{} = \"{}\" and aut.{} = \"{}\" and ent.{} = \"{}\"
                CREATE (e) -[:{}]-> (ent) 
                """
                query = CREATE_QUERY.format(LABELS['location_label'], LABELS['edge_to_source'],
                                            LABELS['edge_label'], LABELS['edge_to_target'],
                                            LABELS['location_label'], LABELS['has'],
                                            LABELS['automaton_label'], ':'.join(entity_labels),
                                            LABELS['location_attr']['name'], edge.source.name,
                                            LABELS['edge_attr']['event'], edge.label,
                                            LABELS['location_attr']['name'], edge.target.name,
                                            LABELS['automaton_attr']['name'], automaton.name,
                                            SCHEMA['entity_properties']['id'], ent.entity_id, name)
        elif loc is not None:
            CREATE_QUERY = """
            MATCH (l:{}) -[:{}]-> (aut:{}), (ent:{})
            WHERE l.{} = \"{}\" and aut.{} = \"{}\" and ent.{} = \"{}\"
            CREATE (l) -[:{}]-> (ent) 
            """
            query = CREATE_QUERY.format(LABELS['location_label'], LABELS['has'],
                                        LABELS['automaton_label'], ':'.join(entity_labels),
                                        LABELS['location_attr']['name'], loc.name,
                                        LABELS['automaton_attr']['name'], automaton.name,
                                        SCHEMA['entity_properties']['id'], ent.entity_id, name)

        with self.driver.session() as session:
            session.run(query)
