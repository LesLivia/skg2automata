import configparser
import json

from neo4j import Driver, Result
from tqdm import tqdm

from it.polimi.model.schema import Event, Entity, Sensor

config = configparser.ConfigParser()
config.read('./resources/config/config.ini')
config.sections()

SCHEMA_PATH = config['NEO4J SCHEMA']['schema.path'].format(config['NEO4J SCHEMA']['schema.name'])


class Ekg_Querier:
    def __init__(self, driver: Driver):
        self.driver = driver
        f = open(SCHEMA_PATH)
        self.schema = json.load(f)

    def get_events(self):
        with self.driver.session() as session:
            events_recs: Result = session.run("MATCH (e:{}) RETURN e".format(self.schema['event']))
            return [Event.parse_evt(e, self.schema['event_properties']) for e in
                    tqdm(events_recs.data())]

    def get_entities(self):
        with self.driver.session() as session:
            entities = session.run("MATCH (e:{}) RETURN e".format(self.schema['entity']))
            return [Entity.parse_ent(e, self.schema['entity_properties']) for e in tqdm(entities.data())]

    def get_sensors(self):
        with self.driver.session() as session:
            sensors = session.run("MATCH (s:{}) RETURN s".format(self.schema['sensor']))
            return [Sensor.parse_sns(s, self.schema['sensor_properties']) for s in tqdm(sensors.data())]
