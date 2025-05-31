import os
from typing import List, Optional

from ..data_managers.semantic_header import SemanticHeader
from ..data_managers.datastructures import DatasetDescriptions
from ..modules.db_management import DBManagement
from ..modules.ekg_builder_semantic_header import EKGUsingSemanticHeaderBuilder
from ..modules.data_importer import Importer
from ..database_managers.db_connection import DatabaseConnection
from ..utilities.performance_handling import Performance


class OcedPg:

    def __init__(self, database_connection: DatabaseConnection, dataset_descriptions: DatasetDescriptions,
                 semantic_header: SemanticHeader,
                 use_sample: bool = False,
                 use_preprocessed_files: bool = False,
                 store_files: bool = True,
                 import_directory=None):
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
        self.data_importer = Importer(database_connection=database_connection,
                                      data_structures=dataset_descriptions,
                                      use_sample=use_sample,
                                      use_preprocessed_files=use_preprocessed_files,
                                      semantic_header=semantic_header,
                                      import_directory=import_directory,
                                      store_files=store_files)
        self.ekg_builder = EKGUsingSemanticHeaderBuilder(database_connection=database_connection,
                                                         semantic_header=semantic_header)
        self.db_manager = DBManagement(db_connection=database_connection)
        self.semantic_header = semantic_header
        self.dataset_descriptions = dataset_descriptions

    def load_and_transform(self):
        self.load()
        self.transform()

    # region import events
    def load(self, logs=None):
        """
        Import data, both event data as other data, as specified in the DS files.
        Add record labels as specified in the semantic header
        Args:
            logs: list of logs to be imported
        """

        # only import logs that are not imported yet
        # dataset name is considered to be unique
        already_imported_logs = self.db_manager.get_imported_logs()
        if logs is None: # no logs are predefined
            logs = self.dataset_descriptions.get_files_list()
        to_be_imported_logs = [log for log in logs if log not in already_imported_logs]

        return self.data_importer.import_data(to_be_imported_logs=to_be_imported_logs)

    # endregion
    def transform(self, logs=None):
        """
            Transform the record layer into a semantic layer
        """
        self.create_nodes(logs)
        self.create_relations(logs)

    def create_nodes(self, node_types: Optional[List[str]] = None, logs: Optional[List[str]] = None) -> None:
        """
        Create nodes as specified in the semantic header

        Args:
            node_types: list of nodes that should be created. In case of None, all nodes are
            created as specified in the semantic header
            logs: list of logs that need to be transformed

        """
        self.create_nodes_by_records(node_types=node_types, logs=logs)

    def create_nodes_by_records(self, node_types: Optional[List[str]] = None,
                                logs: Optional[List[str]] = None) -> None:
        """
        Create nodes based on records as specified in the semantic header

        Args:
            node_types: list of nodes that should be created. In case of None, all nodes are
            created as specified in the semantic header
            logs: list of logs that need to be transformed

        """
        if logs is None:
            files = self.dataset_descriptions.get_structure_name_file_mapping()
            for name, files in files.items():
                self.ekg_builder.create_nodes_by_records(node_types, logs=files)
        else:
            self.ekg_builder.create_nodes_by_records(node_types, logs=logs)

    def create_relations(self, relation_types: Optional[List[str]] = None,
                         logs: Optional[List[str]] = None) -> None:
        """
        Create relations as specified in the semantic header

        Args:
            relation_types: list of relations that should be created. In case of None, all relations are
            created as specified in the semantic header
            logs: list of logs that need to be transformed

        """
        if logs is None:
            files = self.dataset_descriptions.get_structure_name_file_mapping()
            for name, files in files.items():
                self.create_relations_using_record(relation_types=relation_types, logs=files)
                self.create_relations_using_relations(relation_types=relation_types)
                self.create_relations_using_nodes(relation_types=relation_types)
        else:
            self.create_relations_using_record(relation_types=relation_types, logs=logs)
            self.create_relations_using_relations(relation_types=relation_types)
            self.create_relations_using_nodes(relation_types=relation_types)

    def create_relations_using_record(self, relation_types: Optional[List[str]] = None,
                                      logs: Optional[List[str]] = None) -> None:
        """
        Create relations using records as specified in the semantic header

        Args:
            relation_types: list of relations that should be created. In case of None, all relations are
            created as specified in the semantic header
            logs: list of logs that need to be transformed

        """
        self.ekg_builder.create_relations_using_records(relation_types=relation_types, logs=logs)

    def create_relations_using_relations(self, relation_types: Optional[List[str]] = None) -> None:
        """
        Create relations using a subgraph as specified in the semantic header

        Args:
            relation_types: list of relations that should be created. In case of None, all relations are
            created as specified in the semantic header

        """
        self.ekg_builder.create_relations_using_relations(relation_types)

    def create_relations_using_nodes(self, relation_types: Optional[List[str]] = None) -> None:
        """
        Create relations using a subgraph as specified in the semantic header

        Args:
            relation_types: list of relations that should be created. In case of None, all relations are
            created as specified in the semantic header

        """
        self.ekg_builder.create_relations_using_nodes(relation_types)

    def create_nodes_by_relations(self, node_types: Optional[List[str]] = None) -> None:
        """
        Pass on method to ekg_builder to create entities based on relations as specified in the semantic header

        :param node_types: list of entity types for which the entities based on relations should be created. In
        case of None, all entities based on relations are created as specified in the semantic header
        :type node_types: List[str], optional
        :return: None

        """
        self.ekg_builder.create_nodes_by_relations(node_types)

    def create_df_edges(self, entity_types: Optional[List[str]] = None, event_label: str = "Event",
                        add_duration=False) -> None:
        """
        Pass on method to ekg_builder to merge parallel directly follows in between batching events

        :return: None
        """
        self.ekg_builder.create_df_edges(entity_types, event_label, add_duration=add_duration)
        self.ekg_builder.merge_duplicate_df()

    def create_static_nodes_and_relations(self) -> None:
        """
        Pass on method to ekg_builder to create static nodes and relations
        No implementation yet

        :return: None
        """
        self.ekg_builder.create_static_nodes_and_relations()
