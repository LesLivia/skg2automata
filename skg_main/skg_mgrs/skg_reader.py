import configparser
import json
import os
from typing import List, Tuple

from neo4j import Driver, Result

from skg_main.skg_model.automata import TimeDistr
from skg_main.skg_model.schema import Event, Entity, Activity
from skg_main.skg_model.semantics import EntityTree, EntityRelationship, EntityForest

config = configparser.ConfigParser()
config.read(os.path.dirname(os.path.abspath(__file__)).split('skg_main')[0] + 'skg_main/resources/config/config.ini')
config.sections()


class Skg_Reader:
    def setup(self):
        NEO4J_CONFIG = config['NEO4J INSTANCE']['instance']

        if NEO4J_CONFIG.lower() == 'env_var':
            SCHEMA_NAME = os.environ['NEO4J_SCHEMA']
        else:
            SCHEMA_NAME = config['NEO4J SCHEMA']['schema.name']

        SCHEMA_PATH = config['NEO4J SCHEMA']['schema.path'].format(
            os.path.dirname(os.path.abspath(__file__)).split('skg_main')[0] + 'skg_main', SCHEMA_NAME)
        SCHEMA = json.load(open(SCHEMA_PATH))

        LABELS_PATH = config['AUTOMATA TO SKG']['labels.path'].format(
            os.path.dirname(os.path.abspath(__file__)).split('skg_main')[0] + 'skg_main')
        SHA_LABELS = json.load(open(LABELS_PATH))

        return SCHEMA, SHA_LABELS

    def __init__(self, driver: Driver):
        self.driver = driver
        self.SCHEMA, self.SHA_LABELS = self.setup()

    def get_events(self):
        with self.driver.session() as session:
            events_recs: Result = session.run("MATCH (e:{}) RETURN e".format(self.SCHEMA['event']))
            return [Event.parse_evt(e, self.SCHEMA['event_properties']) for e in events_recs.data()]

    def get_unique_events(self):
        all_events = self.get_events()
        unique_events = [e.activity for e in all_events]
        return set(unique_events)

    def get_events_by_timestamp(self, start_t: int = None, end_t: int = None):
        query = ""

        version_filter = ''
        if 'version' in self.SCHEMA:
            version_filter = 'and e:{}'.format(self.SCHEMA['version'])

        if start_t is not None and end_t is None:
            query = "MATCH (e:{}) WHERE e.{} > {} {} RETURN e " \
                    "ORDER BY e.{}".format(self.SCHEMA['event'], self.SCHEMA['event_properties']['timestamp'],
                                           str(start_t), version_filter, self.SCHEMA['event_properties']['timestamp'])
        elif start_t is None and end_t is not None:
            query = "MATCH (e:{}) WHERE e.{} < {} {} RETURN e " \
                    "ORDER BY e.{}".format(self.SCHEMA['event'], self.SCHEMA['event_properties']['timestamp'],
                                           str(end_t), version_filter, self.SCHEMA['event_properties']['timestamp'])
        elif start_t is not None and end_t is not None:
            query = "MATCH (e:{}) where e.{} > {} and e.{} < {} {} return e " \
                    "ORDER BY e.{}".format(self.SCHEMA['event'], self.SCHEMA['event_properties']['timestamp'],
                                           str(start_t), self.SCHEMA['event_properties']['timestamp'],
                                           str(end_t), version_filter, self.SCHEMA['event_properties']['timestamp'])
        with self.driver.session() as session:
            events_recs: Result = session.run(query)
            return [Event.parse_evt(e, self.SCHEMA['event_properties']) for e in events_recs.data()]

    def get_events_by_date(self, start_t=None, end_t=None):
        query = ""

        if 'date' not in self.SCHEMA['event_properties']:
            return self.get_events_by_timestamp(start_t, end_t)

        if start_t is None and end_t is None:
            return self.get_events()

        version_filter = ''
        if 'version' in self.SCHEMA:
            version_filter = 'and e:{}'.format(self.SCHEMA['version'])

        if self.SCHEMA["date_format"] == 'ISO8601':
            date_format = "apoc.date.fromISO8601(\"{}\")"
        else:
            date_format = "apoc.date.parse(\"{}\", \"ms\", \"" + self.SCHEMA["date_format"] + "\")"

        if start_t is not None and end_t is None:
            query = "MATCH (e:{}) WHERE e.{} > datetime({{epochmillis: {}}}) {} RETURN e " \
                    "ORDER BY e.{}".format(self.SCHEMA['event'], self.SCHEMA['event_properties']['date'],
                                           date_format.format(start_t.format(self.SCHEMA["date_format"])),
                                           version_filter,
                                           self.SCHEMA['event_properties']['date'])
        elif start_t is None and end_t is not None:
            query = "MATCH (e:{}) WHERE e.{} < datetime({{epochmillis: {}}}) {} RETURN e " \
                    "ORDER BY e.{}".format(self.SCHEMA['event'], self.SCHEMA['event_properties']['date'],
                                           date_format.format(end_t.format(self.SCHEMA["date_format"])), version_filter,
                                           self.SCHEMA['event_properties']['date'])
        elif start_t is not None and end_t is not None:
            query = "MATCH (e:{}) where e.{} > datetime({{epochmillis: {}}}) and " \
                    "e.{} < datetime({{epochmillis: {}}}) {} RETURN e " \
                    "ORDER BY e.{}".format(self.SCHEMA['event'], self.SCHEMA['event_properties']['date'],
                                           date_format.format(start_t.format(self.SCHEMA["date_format"])),
                                           self.SCHEMA['event_properties']['date'],
                                           date_format.format(end_t.format(self.SCHEMA["date_format"])), version_filter,
                                           self.SCHEMA['event_properties']['date'])
        with self.driver.session() as session:
            events_recs: Result = session.run(query)
            return [Event.parse_evt(e, self.SCHEMA['event_properties']) for e in events_recs.data()]

    def get_events_by_entity(self, en_id: str, pov: str = 'item'):
        arc = self.SCHEMA['event_to_item'] if pov.lower() == 'item' else self.SCHEMA['event_to_resource']

        version_filter = ''
        if 'version' in self.SCHEMA:
            version_filter = 'and e:{}'.format(self.SCHEMA['version'])

        # FIXME not great, preferable if a property is a primary key for any self.schema.
        if self.SCHEMA['entity_properties']['id'] != 'ID':
            query = "MATCH (e:{}) - [:{}] - (y:{}) WHERE toString(y.{}) = \"{}\" {} RETURN e " \
                    "ORDER BY e.{}".format(self.SCHEMA['event'], arc, self.SCHEMA['entity'],
                                           self.SCHEMA['entity_properties']['id'], en_id, version_filter,
                                           self.SCHEMA['event_properties']['timestamp'])
        else:
            query = "MATCH (e:{}) - [:{}] - (y:{}) WHERE toString(ID(y)) = \"{}\" {} RETURN e " \
                    "ORDER BY e.{}".format(self.SCHEMA['event'], arc, self.SCHEMA['entity'],
                                           en_id, version_filter, self.SCHEMA['event_properties']['timestamp'])
        with self.driver.session() as session:
            events_recs: Result = session.run(query)
            return [Event.parse_evt(e, self.SCHEMA['event_properties']) for e in events_recs.data()]

    def get_timestamp_filter(self, start_t, end_t, e_id='e'):
        if self.SCHEMA["date_format"] == 'ISO8601':
            date_format = "apoc.date.fromISO8601(\"{}\")"
        else:
            date_format = "apoc.date.parse(\"{}\", \"ms\", \"" + self.SCHEMA["date_format"] + "\")"

        if start_t is not None and end_t is None:
            timestamp_filter = "{}.{} > datetime({{epochmillis: {}}})" \
                .format(e_id, self.SCHEMA['event_properties']['timestamp'],
                        date_format.format(start_t.format(self.SCHEMA["date_format"])))
        elif start_t is None and end_t is not None:
            timestamp_filter = "{}.{} < datetime({{epochmillis: {}}})" \
                .format(e_id, self.SCHEMA['event_properties']['timestamp'],
                        date_format.format(end_t.format(self.SCHEMA["date_format"])))
        elif start_t is not None and end_t is not None:
            timestamp_filter = "{}.{} > datetime({{epochmillis: {}}}) and {}.{} < datetime({{epochmillis: {}}})" \
                .format(e_id, self.SCHEMA['event_properties']['timestamp'],
                        date_format.format(start_t.format(self.SCHEMA["date_format"])),
                        e_id, self.SCHEMA['event_properties']['timestamp'],
                        date_format.format(end_t.format(self.SCHEMA["date_format"])))

        return timestamp_filter

    def get_events_by_entity_and_timestamp(self, en_id: str, start_t=None, end_t=None, pov: str = 'item'):
        if start_t is None and end_t is None:
            return self.get_events_by_entity(en_id, pov)

        arc = self.SCHEMA['event_to_item'] if pov.lower() == 'item' else self.SCHEMA['event_to_resource']

        timestamp_filter = ""
        if 'date' not in self.SCHEMA['event_properties']:
            if start_t is not None and end_t is None:
                timestamp_filter = "e.{} > {}".format(self.SCHEMA['event_properties']['timestamp'], str(start_t))
            elif start_t is None and end_t is not None:
                timestamp_filter = "e.{} < {}".format(self.SCHEMA['event_properties']['timestamp'], str(end_t))
            elif start_t is not None and end_t is not None:
                timestamp_filter = "e.{} > {} and e.{} < {}".format(self.SCHEMA['event_properties']['timestamp'],
                                                                    str(start_t),
                                                                    self.SCHEMA['event_properties']['timestamp'],
                                                                    str(end_t))
        else:
            timestamp_filter = self.get_timestamp_filter(start_t, end_t)

        # FIXME not great, preferable if a property is a primary key for any self.schema.
        if self.SCHEMA['entity_properties']['id'] != 'ID':
            query = "MATCH (e:{}) - [:{}] - (y:{}) WHERE {} and toString(y.{}) = \"{}\" RETURN e " \
                    "ORDER BY e.{}".format(self.SCHEMA['event'], arc, self.SCHEMA['entity'], timestamp_filter,
                                           self.SCHEMA['entity_properties']['id'], en_id,
                                           self.SCHEMA['event_properties']['timestamp'])
        else:
            query = "MATCH (e:{}) - [:{}] - (y:{}) WHERE {} and toString(ID(y)) = \"{}\" RETURN e " \
                    "ORDER BY e.{}".format(self.SCHEMA['event'], arc, self.SCHEMA['entity'], timestamp_filter,
                                           en_id, self.SCHEMA['event_properties']['timestamp'])

        with self.driver.session() as session:
            events_recs: Result = session.run(query)
            return [Event.parse_evt(e, self.SCHEMA['event_properties']) for e in events_recs.data()]

    def get_events_by_entity_tree(self, tree: EntityTree, pov: str = 'item'):
        events: List[Event] = []
        for node in tree.nodes:
            events.extend(self.get_events_by_entity(node.entity_id, pov))
        events.sort(key=lambda e: e.timestamp)
        return events

    def get_events_by_entity_tree_and_timestamp(self, tree: EntityTree, start_t, end_t, pov: str = 'item'):
        events: List[Event] = []
        for node in tree.nodes:
            events.extend(self.get_events_by_entity_and_timestamp(node.entity_id, start_t, end_t, pov))
        events.sort(key=lambda e: e.timestamp)
        return events

    def get_entities(self, limit: int = None, random: bool = False):
        query = "MATCH (e:{}) RETURN e".format(self.SCHEMA['entity'])

        if random:
            query = query + ', rand() as r ORDER BY r'

        if limit is not None:
            query = query + ' LIMIT {}'.format(limit)

        with self.driver.session() as session:
            entities = session.run(query)
            return [Entity.parse_ent(e, self.SCHEMA['entity_properties']) for e in entities.data()]

    def get_entity_by_id(self, entity_id: str):
        if self.SCHEMA['entity_properties']['id'] != 'ID':
            query = "MATCH (e:{}) WHERE toString(e.{}) = \"{}\" RETURN e".format(self.SCHEMA['entity'],
                                                                                 self.SCHEMA['entity_properties']['id'],
                                                                                 entity_id)
        else:
            query = "MATCH (e:{}) WHERE toString(ID(e)) = \"{}\" RETURN e,ID(e)".format(self.SCHEMA['entity'],
                                                                                        entity_id)

        with self.driver.session() as session:
            results = session.run(query)
            # FIXME: not great, preferable if a property is a primary key for any self.schema.
            if self.SCHEMA['entity_properties']['id'] != 'ID':
                entities = [Entity.parse_ent(e, self.SCHEMA['entity_properties']) for e in results.data()]
            else:
                entities = [Entity.parse_ent(e, self.SCHEMA['entity_properties'], neo4_id=e['ID(e)']) for e in
                            results.data()]
            if len(entities) > 0:
                return entities[0]
            else:
                return None

    def get_entities_by_labels(self, labels: List[str] = None, limit: int = None, random: bool = False,
                               start_t=None, end_t=None):

        if labels is None:
            return self.get_entities(limit, random)
        else:
            query_filter = "WHERE " + ' and '.join(["e:{}".format(l) for l in labels])
            if 'version' in self.SCHEMA:
                query_filter += ' and e:{}'.format(self.SCHEMA['version'])

            if start_t is not None and end_t is not None:
                if 'date' not in self.SCHEMA['event_properties']:
                    query_filter += ' and ev.{} >= {} and ev.{} <= {}'.format(
                        self.SCHEMA['event_properties']['timestamp'],
                        start_t,
                        self.SCHEMA['event_properties']['timestamp'],
                        end_t)
                else:
                    query_filter += ' and ' + self.get_timestamp_filter(start_t, end_t, 'ev')

        if start_t is None and end_t is None:
            query = "MATCH (e:{}) {} RETURN ID(e), e".format(self.SCHEMA['entity'], query_filter)
        else:
            query = "MATCH (e:{}) <-[:{}]- (ev:{}) {} RETURN ID(e), e".format(self.SCHEMA['entity'],
                                                                              self.SCHEMA['event_to_item'],
                                                                              self.SCHEMA['event'],
                                                                              query_filter)

        if random:
            query = query + ', rand() as r ORDER BY r'

        if limit is not None:
            query = query + ' LIMIT {}'.format(limit)

        with self.driver.session() as session:
            entities = session.run(query)
            # FIXME: not great, preferable if a property is a primary key for any self.schema.
            if self.SCHEMA['entity_properties']['id'] != 'ID':
                return [Entity.parse_ent(e, self.SCHEMA['entity_properties']) for e in entities.data()]
            else:
                return [Entity.parse_ent(e, self.SCHEMA['entity_properties'], neo4_id=e['ID(e)']) for e in
                        entities.data()]

    def get_entity_labels_hierarchy(self):
        if 'entity_to_entity' not in self.SCHEMA:
            return [[l] for l in self.SCHEMA['entity_labels']]

        IGNORE_LABELS = [self.SCHEMA['entity'], self.SCHEMA['run']]
        version_filter = ''
        if 'version' in self.SCHEMA:
            IGNORE_LABELS.append(self.SCHEMA['version'])
            version_filter += 'WHERE e1:{}'.format(self.SCHEMA['version'])

        query = "MATCH (e1:{}) - [:{}] -> (e2:{}) {} RETURN labels(e1), labels(e2)".format(self.SCHEMA['entity'],
                                                                                           self.SCHEMA[
                                                                                               'entity_to_entity'],
                                                                                           self.SCHEMA['entity'],
                                                                                           version_filter)
        with self.driver.session() as session:
            results = session.run(query)

            rels: List[Tuple[str, str]] = []
            for res in results.data():
                rels.append(('-'.join([r for r in res['labels(e1)'] if r not in IGNORE_LABELS]),
                             '-'.join([r for r in res['labels(e2)'] if r not in IGNORE_LABELS])))

            return EntityTree.get_labels_hierarchy(set(rels))

    def get_items(self, labels_hierarchy=None, limit: int = None, random: bool = False, start_t=None, end_t=None):
        if labels_hierarchy is None:
            labels_hierarchy: List[List[str]] = self.get_entity_labels_hierarchy()
        if 'item' in self.SCHEMA:
            labels_seq = [seq for seq in labels_hierarchy if self.SCHEMA['item'] in seq][0]
            entities: List[Entity] = []
            for label in labels_seq:
                entities.extend(self.get_entities_by_labels(label.split('-'), limit, random, start_t, end_t))
            return entities
        else:
            return self.get_entities(limit, random)

    def get_resource_labels_hierarchy(self):
        if 'resource_to_resource' not in self.SCHEMA:
            return [[self.SCHEMA['resource']]]

        query = "MATCH (e1:{}) - [:{}] -> (e2:{}) RETURN labels(e1), labels(e2)".format(self.SCHEMA['resource'],
                                                                                        self.SCHEMA[
                                                                                            'resource_to_resource'],
                                                                                        self.SCHEMA['resource'])
        with self.driver.session() as session:
            results = session.run(query)

            rels: List[Tuple[str, str]] = []
            for res in results.data():
                rels.append(('-'.join([r for r in res['labels(e1)']]),
                             '-'.join([r for r in res['labels(e2)']])))

            return EntityTree.get_labels_hierarchy(set(rels))

    def get_resources(self, labels_hierarchy=None, limit: int = None, random: bool = False):
        if labels_hierarchy is None:
            labels_hierarchy: List[List[str]] = self.get_resource_labels_hierarchy()
        if 'resource' in self.SCHEMA:
            unpacked_labels_seq = [[label.split('-') for label in seq if '-' in label] for seq in labels_hierarchy]
            [labels_hierarchy.extend(labels) for labels in unpacked_labels_seq if len(labels) > 0]
            labels_seq = [seq for seq in labels_hierarchy if self.SCHEMA['resource'] in seq][0]
            entities: List[Entity] = self.get_entities_by_labels(labels_seq, limit, random)
            return entities
        else:
            return self.get_entities(limit, random)

    def get_entity_forest(self, labels_hierarchy: List[List[str]]):
        # WARNING: Builds tree for every entity in the KG, likely computational intensive.
        query_tplt = "MATCH (e1:{}) - [:{}] -> (e2:{}) WHERE {} RETURN e1, e2"
        trees: EntityForest = EntityForest([])
        for seq_i, seq in enumerate(labels_hierarchy):
            for i in range(len(seq) - 1, -1, -1):
                query_filter = 'e2:{}'.format(seq[i].split('-')[0]) + ''.join(
                    [' and e2:{}'.format(s) for s in seq[i].split('-')[1:]])
                query = query_tplt.format(self.SCHEMA['entity'], self.SCHEMA['entity_to_entity'], self.SCHEMA['entity'],
                                          query_filter)
                with self.driver.session() as session:
                    results = session.run(query)
                    entities: List[Tuple[Entity, Entity]] = [
                        (Entity.parse_ent(r, self.SCHEMA['entity_properties'], 'e2'),
                         Entity.parse_ent(r, self.SCHEMA['entity_properties'], 'e1'))
                        for r in results.data()]
                    if len(entities) == 0:
                        continue

                    new_rels: List[EntityRelationship] = [EntityRelationship(tup[0], tup[1]) for tup in entities]
                    trees.add_trees([EntityTree([rel]) for rel in new_rels])

        return trees

    def get_entity_tree(self, entity_id: str, trees: EntityForest, reverse: bool = False):
        if 'entity_to_entity' not in self.SCHEMA:
            root_tree = EntityTree([])
            entity = self.get_entity_by_id(entity_id)
            if entity is None:
                return trees
            root_tree.nodes[entity] = []
            trees.add_trees([root_tree])
            return trees

        version_filter = ''
        if 'version' in self.SCHEMA:
            version_filter = ' and e1:{}'.format(self.SCHEMA['version'])

        if reverse:
            query_tplt = "MATCH (e1:{}) <- [:{}] - (e2:{}) "
        else:
            query_tplt = "MATCH (e1:{}) - [:{}] -> (e2:{}) "

        query = query_tplt.format(self.SCHEMA['entity'], self.SCHEMA['entity_to_entity'], self.SCHEMA['entity'])
        if self.SCHEMA['entity_properties']['id'] != 'ID':
            query += "WHERE toString(e2.{}) = \"{}\" {} RETURN e1,e2".format(self.SCHEMA['entity_properties']['id'],
                                                                             entity_id, version_filter)
        else:
            query += "WHERE ID(e2) = {} {} RETURN e1,e2".format(entity_id, version_filter)

        with self.driver.session() as session:
            results = session.run(query)
            entities: List[Tuple[Entity, Entity]] = [(Entity.parse_ent(r, self.SCHEMA['entity_properties'], 'e2'),
                                                      Entity.parse_ent(r, self.SCHEMA['entity_properties'], 'e1'))
                                                     for r in results.data()]
        if len(entities) == 0:
            root_tree = EntityTree([])
            entity = self.get_entity_by_id(entity_id)
            if entity is None:
                return trees
            root_tree.nodes[entity] = []
            trees.add_trees([root_tree])
            return trees

        new_rels: List[EntityRelationship] = [EntityRelationship(tup[0], tup[1]) for tup in entities]
        trees.add_trees([EntityTree([rel]) for rel in new_rels])
        children = [e[1].entity_id for e in entities]
        for child in children:
            self.get_entity_tree(child, trees, reverse)
        return trees

    def get_activities(self):
        version_filter = ''
        if 'version' in self.SCHEMA:
            version_filter = "WHERE s:{}".format(self.SCHEMA['version'])

        with self.driver.session() as session:
            activities = session.run("MATCH (s:{}) {} RETURN s".format(self.SCHEMA['activity'], version_filter))
            return [Activity.parse_act(s, self.SCHEMA['activity_properties']) for s in activities.data()]

    def get_related_entities(self, entity_from: str = None, entity_to: str = None,
                             filter1: str = None, filter2: str = None,
                             limit: int = None, random: bool = False):
        query = ""
        if entity_from is None:
            query += 'MATCH (e1:{})'.format(self.SCHEMA['Entity'])
        else:
            query += 'MATCH (e1:{})'.format(entity_from)

        if entity_from is None or entity_to is None:
            query += ' -> '
        else:
            for rels in self.SCHEMA['custom_entity_to_entity']:
                if rels['from'] == entity_from and rels['to'] == entity_to:
                    query += ' -[:{}]-> '.format(rels['name'])
                    break

        if entity_to is None:
            query += ' (e2:{}) '.format(self.SCHEMA['Entity'])
        else:
            query += ' (e2:{}) '.format(entity_to)

        if filter1 is not None:
            query += ' WHERE e1.{}=\"{}\" '.format(self.SCHEMA['entity_properties']['id'], filter1)

        if filter1 is None and filter2 is not None:
            query += ' WHERE e2.{}=\"{}\" '.format(self.SCHEMA['entity_properties']['id'], filter2)
        elif filter1 is not None and filter2 is not None:
            if filter1 is not None:
                query += ' and e2.{}=\"{}\" '.format(self.SCHEMA['entity_properties']['id'], filter1)

        query += 'RETURN e1,e2'

        if random:
            query += ',rand() as r ORDER BY r'

        if limit is not None:
            query += ' LIMIT {}'.format(limit)

        with self.driver.session() as session:
            results = session.run(query)
            entities: List[Tuple[Entity, Entity]] = [(Entity.parse_ent(r, self.SCHEMA['entity_properties'], 'e2'),
                                                      Entity.parse_ent(r, self.SCHEMA['entity_properties'], 'e1'))
                                                     for r in results.data()]

        return entities

    def get_invariants(self, automaton_name: str, start: int, end: int, loc_name: str):
        query = """MATCH (a:{}) <-[:{}]- (l:{}:{}) -[:MODELS]-> (s:{}) -[:APPLIES]-> (f:{}) 
        -[:OUTPUT]-> (e:{}), (g:GraphModel:Instance)
        WHERE a.{} = '{}' and a.{} = '{}' and a.{} = '{}' and l.{} = '{}'
        RETURN l,s,f,e"""
        query = query.format(self.SHA_LABELS["automaton_label"], self.SHA_LABELS["has"],
                             self.SHA_LABELS["automaton_feature"], self.SHA_LABELS["location_label"],
                             self.SCHEMA["resource"], self.SCHEMA["res_time_distr"], self.SCHEMA["entity_type"],
                             self.SHA_LABELS["automaton_attr"]["name"], automaton_name,
                             self.SHA_LABELS["automaton_attr"]["start"], start,
                             self.SHA_LABELS["automaton_attr"]["end"], end,
                             self.SHA_LABELS["location_attr"]["name"], loc_name)

        with self.driver.session() as session:
            results = session.run(query)
            entities: List[TimeDistr] = [TimeDistr(r['e']['code'], r['s']['sysId'],
                                                   {a: r['f'][self.SCHEMA['res_time_distr_attr'][a]] for a in
                                                    self.SCHEMA['res_time_distr_attr']}) for r in results.data()]

        return entities

    def get_prob_weights(self, automaton_name: str, start: int, end: int,
                         sync: str, source_name: str):
        query = """MATCH (a:{}) <-[:{}]- (e:{}:{}) -[:LABELED_BY]-> 
        (s:MachinePart:Sensor) -[:PART_OF]-> (st:{}), (c:Connection:Ensemble) <-[:BELONGS_TO]- 
        (r:{}) <-[:OCCUPIES]- (et:{}), (st2:{}) <-[:MODELS]- (src:{}:{}) -[:{}]-> (e)
        WHERE a.{}='{}' and a.{}='{}' and a.{}='{}' and e.{}='{}' and src.{}='{}'
        and (st) <-[:DESTINATION]- (c) and (st2) -[:ORIGIN]-> (c)
        RETURN e, s, st, c, r, et, src, st2"""
        query = query.format(self.SHA_LABELS["automaton_label"], self.SHA_LABELS["has"],
                             self.SHA_LABELS["automaton_feature"],
                             self.SHA_LABELS["edge_label"], self.SCHEMA["resource"], self.SCHEMA["route"],
                             self.SCHEMA["entity_type"], self.SCHEMA["resource"],
                             self.SHA_LABELS["automaton_feature"], self.SHA_LABELS["location_label"],
                             self.SHA_LABELS["edge_to_source"], self.SHA_LABELS["automaton_attr"]["name"],
                             automaton_name,
                             self.SHA_LABELS["automaton_attr"]["start"], start,
                             self.SHA_LABELS["automaton_attr"]["end"], end,
                             self.SHA_LABELS["edge_attr"]["event"], sync, self.SHA_LABELS["location_attr"]["name"],
                             source_name)

        with self.driver.session() as session:
            results = session.run(query)
            entities: List[Tuple[float, TimeDistr]] = [(float(r['r'][self.SCHEMA['route_attr']['probability']]),
                                                        TimeDistr(r['et']['code'], r['st']['sysId'],
                                                                  {a: r['r'][self.SCHEMA['route_attr'][a]] for a in
                                                                   self.SCHEMA['route_attr']})) for r in
                                                       results.data()]

        return entities
