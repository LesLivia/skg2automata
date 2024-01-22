import configparser
import json
import os

from neo4j import Driver

from src.skg2automata.logger.logger import Logger
from src.skg2automata.model.automata import Automaton

config = configparser.ConfigParser()
if 'submodules' in os.listdir():
    curr_path = os.getcwd() + '/submodules/skg2automata'
else:
    curr_path = os.getcwd().split('src/skg2automata')[0]
config.read('{}/resources/config/config.ini'.format(curr_path))
config.sections()

LABELS_PATH = config['AUTOMATA TO SKG']['labels.path'].format(curr_path)
LABELS = json.load(open(LABELS_PATH))

SCHEMA_NAME = config['NEO4J SCHEMA']['schema.name']
SCHEMA_PATH = config['NEO4J SCHEMA']['schema.path'].format(curr_path, SCHEMA_NAME)
SCHEMA = json.load(open(SCHEMA_PATH))

AUTOMATON_PATH = config['AUTOMATA TO SKG']['automaton.path']

LOGGER = Logger('SKG Writer')


class Skg_Writer:
    def __init__(self, driver: Driver):
        self.driver = driver

    def write_automaton(self):
        LOGGER.info('Loading {}...'.format(AUTOMATON_PATH))
        automaton = Automaton(filename=AUTOMATON_PATH)
        LOGGER.info('Found {} locations, {} edges.'.format(len(automaton.locations), len(automaton.edges)))

        AUTOMATON_NAME = AUTOMATON_PATH.split('/')[-1].split('.')[0]
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
            WHERE s.{} = \"{}\" and t.{} = \"{}\" and a.{} = \"{}\"
            CREATE (s) -[:{}]-> (e:{}:{} {{ {}: \"{}\" }}) -[:{}]-> (t)
            CREATE (a) <-[:{}]- (e)
        """
        for edge in automaton.edges:
            query = EDGE_TO_LOC_QUERY.format(LABELS['location_label'], LABELS['location_label'],
                                             LABELS['automaton_label'],
                                             LABELS['location_attr']['name'], edge.source.name,
                                             LABELS['location_attr']['name'], edge.target.name,
                                             LABELS['automaton_attr']['name'], AUTOMATON_NAME,
                                             LABELS['edge_to_source'], LABELS['edge_label'],
                                             LABELS['automaton_feature'], LABELS['edge_attr']['event'],
                                             edge.label, LABELS['edge_to_target'], LABELS['has'])
            with self.driver.session() as session:
                session.run(query)
        LOGGER.info("Created Edge nodes.")

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
            LOGGER.info("Deletes {} features.".format(automaton_name))

            DELETE_QUERY = """
            MATCH (a: {})
            WHERE a.{} = \"{}\"
            DETACH DELETE a
            """

            query = DELETE_QUERY.format(LABELS['automaton_label'],
                                        LABELS['automaton_attr']['name'], automaton_name)
            with self.driver.session() as session:
                session.run(query)
            LOGGER.info("Delete {} node.".format(automaton_name))
