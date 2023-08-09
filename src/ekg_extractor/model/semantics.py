from typing import List, Dict, Set, Tuple

from src.ekg_extractor.model.schema import Entity


class EntityRelationship:
    def __init__(self, source: Entity, target: Entity):
        self.source = source
        self.target = target


class EntityHierarchy:
    def __init__(self, arcs: List[EntityRelationship]):
        self.arcs = arcs
        self.nodes: Dict[Entity, List[Entity]] = {}
        for arc in self.arcs:
            if arc.source in self.nodes:
                self.nodes[arc.source].append(arc.target)
            else:
                self.nodes[arc.source] = [arc.target]

            if arc.target not in self.nodes:
                self.nodes[arc.target] = []

    @staticmethod
    def get_labels_tree(arcs: Set[Tuple[str, str]]):
        # TODO: Requires further testing, probably does not support trees with bifurcations.
        def is_part_of_tree(l: str, t: List[List[str]]):
            return any([l in tree for tree in t])

        def pos_in_tree(l: str, t: List[List[str]]):
            return [tree for tree in t if l in tree][0].index(l)

        trees: List[List[str]] = []

        for arc in arcs:
            if is_part_of_tree(arc[0], trees):
                [tree for tree in trees if arc[0] in tree][0].insert(pos_in_tree(arc[0], trees) + 1, arc[1])
            elif is_part_of_tree(arc[1], trees):
                [tree for tree in trees if arc[1] in tree][0].insert(pos_in_tree(arc[1], trees), arc[0])
            else:
                trees.append([arc[0], arc[1]])

        def overlapping_trees(trees: List[List[str]]):
            for i, tree in enumerate(trees):
                for label in tree:
                    for j, tree2 in enumerate(trees):
                        if i != j and label in tree2:
                            return i, j
            return None

        def merge_trees(trees: List[List[str]], i: int, j: int):
            if trees[i][-1] == trees[j][0]:
                trees[i].extend(trees[j][1:])
                trees.pop(j)
            else:
                trees[j].extend(trees[i][1:])
                trees.pop(i)

            return trees

        while overlapping_trees(trees) is not None:
            i, j = overlapping_trees(trees)
            trees = merge_trees(trees, i, j)

        return trees

    def get_root(self):
        for node in self.nodes:
            if len(self.nodes[node]) == 0:
                return node
