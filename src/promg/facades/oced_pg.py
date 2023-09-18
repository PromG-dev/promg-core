import os
from typing import List, Optional

from ..data_managers.semantic_header import SemanticHeader
from ..data_managers.datastructures import DatasetDescriptions
from ..modules.ekg_builder_semantic_header import EKGUsingSemanticHeaderBuilder
from ..modules.data_importer import Importer
from ..utilities.performance_handling import Performance


class OcedPg:

    def __init__(self, dataset_descriptions: DatasetDescriptions,
                 use_sample: bool = False, use_preprocessed_files: bool = False):
        """
            This is a Class that acts as a facade for used to extract, load and transform their data using OCED-PG

            :param dataset_descriptions: A specification of how the data sets are structured
            :type dataset_descriptions: DatasetDescriptions
            :param perf: a :class:Performance to keep track of the running time of the EKG construction
            :type perf: Performance
            :param use_sample: boolean indicating whether the DB should be build using a sample as specified in the
            ImportedDataStructures, defaults to False
            :type use_sample: bool

        """
        # classes responsible for executing queries
        self.data_importer = Importer(data_structures=dataset_descriptions,
                                      use_sample=use_sample,
                                      use_preprocessed_files=use_preprocessed_files)
        self.ekg_builder = EKGUsingSemanticHeaderBuilder()

    def load_and_transform(self):
        self.load()
        self.transform()

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

    def create_df_edges(self, entity_types: Optional[List[str]] = None, event_label: str = "Event") -> None:
        """
        Pass on method to ekg_builder to merge parallel directly follows in between batching events

        :return: None
        """
        self.ekg_builder.create_df_edges(entity_types, event_label)
        self.ekg_builder.merge_duplicate_df()

    def create_static_nodes_and_relations(self) -> None:
        """
        Pass on method to ekg_builder to create static nodes and relations
        No implementation yet

        :return: None
        """
        self.ekg_builder.create_static_nodes_and_relations()
