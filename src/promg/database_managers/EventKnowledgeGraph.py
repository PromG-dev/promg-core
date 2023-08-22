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
from ..utilities.performance_handling import Performance

from tabulate import tabulate
import pandas as pd


# ensure to allocate enough memory to your database: dbms.memory.heap.max_size=5G advised
class EventKnowledgeGraph:
    """
    This is a Class that acts as an interface between the user and the different supported modules

    :param db_connection: A connection to the db
    :type db_connection: DatabaseConnection
    :param db_name: The name of the database
    :type db_name: str
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

    def __init__(self, db_connection: DatabaseConnection, db_name: str,
                 specification_of_data_structures: ImportedDataStructures, perf_path: str,
                 batch_size: int = 5000, use_sample: bool = False, use_preprocessed_files: bool = False,
                 semantic_header: SemanticHeader = None, custom_module_name=None, number_of_steps: int = None):
        # classes responsible for executing queries
        self.ekg_management = DBManagement(db_connection=db_connection, db_name=db_name)
        self.data_importer = Importer(db_connection, data_structures=specification_of_data_structures,
                                      records=semantic_header.records,
                                      batch_size=batch_size,
                                      use_sample=use_sample, use_preprocessed_files=use_preprocessed_files)
        self.ekg_builder = EKGUsingSemanticHeaderBuilder(db_connection=db_connection, semantic_header=semantic_header,
                                                         batch_size=batch_size)
        self.inference_engine = InferenceEngine(db_connection=db_connection)
        self.ekg_analysis = EKGAnalysis(db_connection=db_connection)

        if custom_module_name is not None:
            self.custom_module = custom_module_name(db_connection=db_connection)
        else:
            self.custom_module = None

        self.semantic_header = semantic_header

        self.perf = Performance(perf_path=perf_path, number_of_steps=number_of_steps)

    # region EKG management
    """Define all queries and return their results (if required)"""

    def clear_db(self) -> None:
        """
        Pass on method to ekg_management to clear the entire database

        :return: None
        """

        self.ekg_management.clear_db()

    def set_constraints(self) -> None:
        """
        Pass on method to ekg_management to set the constraints on the model

        :return: None
        """

        self.ekg_management.set_constraints()

    def get_all_rel_types(self) -> List[str]:
        """
        Pass on method to ekg_management to get all possible rel types

        :return: Set of strings
        """
        return self.ekg_management.get_all_rel_types()

    def get_all_node_labels(self) -> Set[str]:
        """
        Pass on method to ekg_management to get all possible node labels
        :return: Set of strings
        """
        return self.ekg_management.get_all_node_labels()

    def get_statistics(self) -> List[Dict[str, str]]:
        """
        Pass on method to ekg_management to get the count of nodes per label and (aggregated) relation types
        :return:  List[Dict[str, str]]
        """
        return self.ekg_management.get_statistics()

    def print_statistics(self) -> None:
        """
        Print the statistics nicely using tabulate
        :return: None
        """
        print(tabulate(self.get_statistics()))

    def get_event_log(self, entity_type: str, additional_event_attributes: Optional[List[str]]):
        """
        Pass on ekg_management to data_imported to get an event log extracted from the EKG for a specific entity and
        return it

        :param entity_type: The type of the entity --> contract states that it exists
        :type entity_type: str
        :param additional_event_attributes: list of different attributes of event that should also be stored in the
        event log
        :type additional_event_attributes: List[str], optional
        :return: List[Dict[str, str]] #TODO check

        :raise ValueError: when the entity has not been defined
        """

        entity_object = self.semantic_header.get_entity(entity_type)
        if entity_object is None:
            raise ValueError(f"Entity {entity_type} is not defined in the semantic header")

        if additional_event_attributes is None:
            additional_event_attributes = []

        return self.ekg_management.get_event_log(entity_object, additional_event_attributes)

    def save_event_log(self, entity_type: str, additional_event_attributes: Optional[List[str]] = None) -> None:
        """
        Create an event log extracted from the EKG from a specific entity and store it as a csv file

        :param entity_type: The type of the entity
        :type entity_type: str
        :param additional_event_attributes: list of different attributes of event that should also be stored in the
        event log
        :type additional_event_attributes: List[str], optional
        :return: None
        """

        event_log = self.get_event_log(entity_type, additional_event_attributes)
        df = pd.DataFrame(event_log)

        current_file_path = os.path.dirname(__file__)

        dir_path = os.path.join(current_file_path, '../../..', '..', 'data', self.semantic_header.name, 'event_logs')
        file_path = os.path.join(dir_path, f"{entity_type}.csv")
        os.makedirs(dir_path, exist_ok=True)
        df.to_csv(file_path, index=True, index_label="idx")

    # endregion

    # region import events
    def import_data(self):
        """
        Pass on method to data_imported to import data, both event data as other data, as specified in the DS files
        :return: None

        """
        self.data_importer.import_data()

    # endregion
    
    def create_nodes(self, node_types: Optional[List[str]] = None) -> None:
        """
        Pass on method to ekg_builder to create relations between entities based on nodes as specified in the
        semantic header

        :param node_types: list of entity types that should be created based on nodes. In case of None,
        all entities based on nodes are created as specified in the semantic header
        :type node_types: List[str], optional
        :return: None

        """

        self.create_nodes_by_records(node_types)

    # region EKG builder using semantic header
    def create_nodes_by_records(self, node_types: Optional[List[str]] = None) -> None:
        """
        Pass on method to ekg_builder to create relations between entities based on nodes as specified in the
        semantic header

        :param node_types: list of entity types that should be created based on nodes. In case of None,
        all entities based on nodes are created as specified in the semantic header
        :type node_types: List[str], optional
        :return: None

        """

        self.ekg_builder.create_nodes_by_records(node_types)
        
    def create_relations(self,  relation_types: Optional[List[str]] = None) -> None:
        self.create_relations_using_record(relation_types)
        self.create_relations_using_relations(relation_types)

    def create_relations_using_record(self, relation_types: Optional[List[str]] = None) -> None:
        """
        Pass on method to ekg_builder to create relations between entities based on nodes as specified in the
        semantic header

        :param relation_types: list of relation types for which the relations should be created. In case of None,
        all relations based on relations are created as specified in the semantic header
        :type relation_types: List[str], optional
        :return: None

        """
        self.ekg_builder.create_relations_using_records(relation_types)

    def create_relations_using_relations(self, relation_types: Optional[List[str]] = None) -> None:
        """
        Pass on method to ekg_builder to create relations between entities based on relations as specified in the
        semantic header

        :param relation_types: list of relation types for which the relations should be created. In case of None,
        all relations based on relations are created for all relation types as specified in the semantic header
        :type relation_types: List[str], optional
        :return: None

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
        Pass on method to ekg_builder to create directly follows edges between events for specific entities

        :param entity_types: list of entity types for which the DFs should be created. In case of None,
        DFs are created for all EntityTypes as specified in the semantic header
        :type entity_types: List[str], optional
        :return: None

        """
        self.ekg_builder.create_df_edges(entity_types, event_label)

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

    def create_static_nodes_and_relations(self) -> None:
        """
        Pass on method to ekg_builder to create static nodes and relations
        No implementation yet

        :return: None
        """
        self.ekg_builder.create_static_nodes_and_relations()

    def match_entity_with_batch_position(self, entity_type: str, relative_position_type: str) -> None:
        """
        Pass on method to inference_engine to infer the batch position of a specific entity

        :param entity_type: The type of the entity
        :type entity_type: str
        :param relative_position_type: The type of the relative position
        :type relative_position_type: str
        :return: None

        :raise ValueError: when the entity has not been defined
        """
        entity = self.semantic_header.get_entity(entity_type)
        relative_position = self.semantic_header.get_entity(relative_position_type)
        if entity is None:
            raise ValueError(f"{entity_type} is not defined in semantic header")
        self.inference_engine.match_entity_with_batch_position(entity, relative_position)

    # rule B
    def infer_items_propagate_downwards_one_level(self, entity_type: str) -> None:
        """
        Pass on method to inference_engine to infer items while propagating downwards one levels

        :param entity_type: The type of the entity
        :type entity_type: str
        :return: None

        :raise ValueError: when the entity has not been defined
        """
        entity = self.semantic_header.get_entity(entity_type)
        if entity_type is None:
            raise ValueError(f"{entity_type} is not defined in semantic header")
        self.inference_engine.infer_items_propagate_downwards_one_level(entity)

    # rule C
    def infer_items_propagate_upwards_multiple_levels(self, entity_type: str, is_load=True) -> None:
        """
        Pass on method to inference_engine to infer items while propagating upwards multiple levels

        :param entity_type: The type of the entity
        :type entity_type: str
        :param is_load: indicating whether we are inferring upwards to load events (true) or unload events (false),
        defaults to True
        :type is_load: bool
        :return: None

        :raise ValueError: when the entity has not been defined
        """

        entity = self.semantic_header.get_entity(entity_type)
        if entity is None:
            raise ValueError(f"{entity_type} is not defined in semantic header")
        self.inference_engine.infer_items_propagate_upwards_multiple_levels(entity, is_load)

    # rule D
    def infer_items_propagate_downwards_multiple_level_w_batching(self, entity_type: str,
                                                                  relative_position_type: str, ) -> None:
        """
        Pass on method to inference_engine to infer items while propagating downwards multiple levels with batching

        :param entity_type: The type of the entity
        :type entity_type: str
        :param relative_position_type: The type of the relative position
        :type relative_position_type: str
        :return: None

        :raise ValueError: when the entity has not been defined in semantic header
        """
        entity = self.semantic_header.get_entity(entity_type)
        relative_position = self.semantic_header.get_entity(relative_position_type)
        if entity_type is None:
            raise ValueError(f"{entity_type} is not defined in semantic header")
        self.inference_engine.infer_items_propagate_downwards_multiple_level_w_batching(entity, relative_position)

    # endregion

    def create_df_process_model(self, entity_type: str):
        """
        Pass on method to ekg_analysis to create a DF process model

        :param entity_type: The type of the entity
        :type entity_type: str
        :param classifiers: the attributes used as classifiers
        :type classifiers: Optional[List[str]], defaults to None

        :return: None

        :raise ValueError: when the entity has not been defined
        """
        entity = self.semantic_header.get_entity(entity_type)
        if entity_type is None:
            raise ValueError(f"{entity_type} is not defined in semantic header")
        self.ekg_analysis.create_df_process_model(entity=entity)

    def do_custom_query(self, query_name: str, **kwargs: Optional[Dict[str, any]]) -> any:
        r"""
        Pass on the custom query to the custom module with optional key words arguments

        :param query_name: the name of the function that contains the query
        :type query_name: str
        :param kwargs: any optional key word argument
        :type kwargs: dict[str, any], optional keyword arguments
        :return: None

        :raise ValueError: when no custom module has been defined
        """
        if self.custom_module is None:
            raise ValueError("No custom module has been defined")
        return self.custom_module.do_custom_query(query_function=query_name, **kwargs)


    def save_perf(self):
        self.perf.finish()
        self.perf.save()
