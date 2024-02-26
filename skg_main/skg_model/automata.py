from typing import List

import pygraphviz as pgv


class Location:
    def __init__(self, name: str):
        self.name = name


class Edge:
    def __init__(self, label: str, source: Location, target: Location):
        self.label = label
        self.source = source
        self.target = target


class Automaton:
    def __init__(self, name: str, filename=None):
        self.name = name
        self.locations: List[Location] = []
        self.edges: List[Edge] = []

        if filename is not None:
            graph = pgv.AGraph(filename)
            for node in graph.nodes():
                name = node.attr['label'].split('>')[1].split('<')[0]
                self.locations.append(Location(name))
            for edge in graph.edges():
                event = edge.attr['label'].split('>')[1].split('<')[0]
                source = [l for l in self.locations if l.name == edge[0]][0]
                target = [l for l in self.locations if l.name == edge[1]][0]
                self.edges.append(Edge(event, source, target))
