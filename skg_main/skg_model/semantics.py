from typing import List, Dict, Set, Tuple

from skg_main.skg_logger.logger import Logger
from skg_main.skg_model.schema import Entity

LOGGER = Logger('EntityTree Mgr')


class EntityRelationship:
    def __init__(self, source: Entity, target: Entity):
        self.source = source
        self.target = target


class EntityTree:
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
    def get_labels_hierarchy(arcs: Set[Tuple[str, str]]):
        # TODO: Requires further testing, probably does not support bifurcations.
        def is_part_of_seq(l: str, t: List[List[str]]):
            return any([l in seq for seq in t])

        def pos_in_seq(l: str, t: List[List[str]]):
            return [seq for seq in t if l in seq][0].index(l)

        seqs: List[List[str]] = []

        for arc in arcs:
            if is_part_of_seq(arc[0], seqs):
                [tree for tree in seqs if arc[0] in tree][0].insert(pos_in_seq(arc[0], seqs) + 1, arc[1])
            elif is_part_of_seq(arc[1], seqs):
                [tree for tree in seqs if arc[1] in tree][0].insert(pos_in_seq(arc[1], seqs), arc[0])
            else:
                seqs.append([arc[0], arc[1]])

        def overlapping_seqs(seqs: List[List[str]]):
            for i, seq in enumerate(seqs):
                for label in seq:
                    for j, seq2 in enumerate(seqs):
                        if i != j and label in seq2:
                            return i, j
            return None

        def concatenate(seqs: List[List[str]], i: int, j: int):
            if seqs[i][-1] == seqs[j][0]:
                seqs[i].extend(seqs[j][1:])
                seqs.pop(j)
            else:
                seqs[j].extend(seqs[i][1:])
                seqs.pop(i)

            return seqs

        while overlapping_seqs(seqs) is not None:
            i, j = overlapping_seqs(seqs)
            seqs = concatenate(seqs, i, j)

        return seqs

    def get_root(self):
        for node in self.nodes:
            if len(self.nodes[node]) == 0:
                return node

    def overlaps_with(self, other):
        for node in self.nodes:
            if node in other.nodes:
                return True
        return False

    @staticmethod
    def merge_trees(t1, t2):
        return EntityTree(t1.arcs + t2.arcs)


class EntityForest:
    def __init__(self, trees: List[EntityTree]):
        self.trees = trees

    def __getitem__(self, item):
        return self.trees[item]

    def overlapping_trees(self):
        for i, t1 in enumerate(self.trees):
            for j, t2 in enumerate(self.trees):
                if i != j and t1.overlaps_with(t2):
                    return i, j
        return None

    def reduce(self):
        tup = self.overlapping_trees()
        while tup is not None:
            i, j = tup[0], tup[1]
            LOGGER.debug('Merging trees {} and {} of {}...'.format(i, j, len(self.trees)))
            self.trees[i] = EntityTree.merge_trees(self.trees[i], self.trees[j])
            self.trees.pop(j)
            tup = self.overlapping_trees()

    def add_trees(self, new_trees: List[EntityTree], reduce: bool = True):
        self.trees.extend(new_trees)
        if reduce:
            self.reduce()
