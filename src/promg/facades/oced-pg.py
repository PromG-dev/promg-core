import os
from typing import List, Set, Optional, Dict

from .db_connection import DatabaseConnection
from ..data_managers.semantic_header import SemanticHeader
from ..data_managers.datastructures import ImportedDataStructures
from ..ekg_modules.ekg_analysis import EKGAnalysis
from ..ekg_modules.ekg_builder_semantic_header import EKGUsingSemanticHeaderBuilder
from ..ekg_modules.db_management import DBManagement
from ..ekg_modules.data_importer import Importer
from ..ekg_modules.inference_engine import InferenceEngine
from ..ekg_modules.task_identification import TaskIdentification
from ..utilities.performance_handling import Performance

from tabulate import tabulate
import pandas as pd


# ensure to allocate enough memory to your database: dbms.memory.heap.max_size=5G advised
class OCEDPG:
    """
    This is a Class that acts as a facade for used to extract, load and transform their data using OCED-PG

    :param specification_of_data_structures: A specification of how the data sets are structured
    :type specification_of_data_structures: ImportedDataStructures
    :param semantic_header: a :class:SemanticHeader class describing the semantics of the EKG
    :type semantic_header: SemanticHeader
    :param perf: a :class:Performance to keep track of the running time of the EKG construction
    :type perf: Performance
    :param batch_size: the batch_size used when calling apoc.periodic.iterate --> reduce required memory, defaults to
    5000
    :type batch_size: int
    :param use_sample: boolean indicating whether the DB should be build using a sample as specified in the
    ImportedDataStructures, defaults to False
    :type use_sample: bool

    """

    def __init__(self, specification_of_data_structures: ImportedDataStructures,
                 batch_size: int = 5000, use_sample: bool = False, use_preprocessed_files: bool = False,
                 semantic_header: SemanticHeader = None):
        # classes responsible for executing queries
        self.data_importer = Importer(data_structures=specification_of_data_structures,
                                      records=semantic_header.records,
                                      batch_size=batch_size,
                                      use_sample=use_sample, use_preprocessed_files=use_preprocessed_files)
        self.ekg_builder = EKGUsingSemanticHeaderBuilder(semantic_header=semantic_header,
                                                         batch_size=batch_size)

    # region import events
    def load(self):
        """
        Import data, both event data as other data, as specified in the DS files.
        Add record labels as specified in the semantic header
        """
        self.data_importer.import_data()

    # endregion
    def transform(self):
        """
            Transform the record layer into a semantic layer
        """
        self.create_nodes()
        self.create_relations()

    def create_nodes(self, node_types: Optional[List[str]] = None) -> None:
        """
        Create nodes as specified in the semantic header

        Args:
            node_types: list of nodes that should be created. In case of None, all nodes are
            created as specified in the semantic header

        """
        self.create_nodes_by_records(node_types)

    def create_nodes_by_records(self, node_types: Optional[List[str]] = None) -> None:
        """
        Create nodes based on records as specified in the semantic header

        Args:
            node_types: list of nodes that should be created. In case of None, all nodes are
            created as specified in the semantic header

        """

        self.ekg_builder.create_nodes_by_records(node_types)

    def create_relations(self, relation_types: Optional[List[str]] = None) -> None:
        """
        Create relations as specified in the semantic header

        Args:
            relation_types: list of relations that should be created. In case of None, all relations are
            created as specified in the semantic header

        """
        self.create_relations_using_record(relation_types)
        self.create_relations_using_relations(relation_types)

    def create_relations_using_record(self, relation_types: Optional[List[str]] = None) -> None:
        """
        Create relations using records as specified in the semantic header

        Args:
            relation_types: list of relations that should be created. In case of None, all relations are
            created as specified in the semantic header

        """
        self.ekg_builder.create_relations_using_records(relation_types)

    def create_relations_using_relations(self, relation_types: Optional[List[str]] = None) -> None:
        """
        Create relations using a subgraph as specified in the semantic header

        Args:
            relation_types: list of relations that should be created. In case of None, all relations are
            created as specified in the semantic header

        """
        self.ekg_builder.create_relations_using_relations(relation_types)

    def create_nodes_by_relations(self, node_types: Optional[List[str]] = None) -> None:
        """
        Pass on method to ekg_builder to create entities based on relations as specified in the semantic header

        :param node_types: list of entity types for which the entities based on relations should be created. In
        case of None, all entities based on relations are created as specified in the semantic header
        :type node_types: List[str], optional
        :return: None

        """
        self.ekg_builder.create_nodes_by_relations(node_types)

    def merge_duplicate_df(self) -> None:
        """
        Pass on method to ekg_builder to merge parallel directly follows in between batching events

        :return: None
        """
        self.ekg_builder.merge_duplicate_df()

    def delete_parallel_dfs_derived(self) -> None:
        """
        Pass on method to ekg_builder to delete parallel directly follows that have been derived when reifying new
        entities

        :return: None
        """
        self.ekg_builder.delete_parallel_dfs_derived()
