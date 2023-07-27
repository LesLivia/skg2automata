from neo4j import Driver


class Ekg_Querier:
    def __init__(self, driver: Driver):
        self.driver = driver

    def get_events(self):
        with self.driver.session() as session:
            events = session.run("MATCH (e:Event) RETURN e")
            return list(events)
