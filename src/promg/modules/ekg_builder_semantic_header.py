from typing import Optional, List, Union

from ..data_managers.semantic_header import ConstructedNodes, ConstructedRelation, Relationship, SemanticHeader, \
    NodeConstructor
from ..database_managers.db_connection import DatabaseConnection
from ..utilities.performance_handling import Performance
from ..cypher_queries.semantic_header_ql import SemanticHeaderQueryLibrary as sh_ql


class EKGUsingSemanticHeaderBuilder:
    def __init__(self, database_connection: DatabaseConnection,
                 semantic_header: SemanticHeader):
        self.connection = database_connection
        self.semantic_header = semantic_header

    def create_nodes_by_records(self, node_types: Optional[List[str]], logs: Optional[List[str]]) -> None:
        # request all associated record labels of the imported logs
        associated_records = self.get_associated_records(logs)

        for node_constructor in self.semantic_header.get_node_by_record_constructors(node_types):
            # check if node_constructor is subset of associated record labels
            # if so, we need to create nodes for this record
            is_subset = set(node_constructor.prevalent_record.record_types).issubset(set(associated_records))
            if logs is None or is_subset:
                self._create_node_by_record(node_constructor=node_constructor, logs=logs)

    def get_associated_records(self, logs):
        # if imported logs, we can return an empty list
        if logs is None:
            return []
        else:
            # request the associated record labels
            result = self.connection.exec_query(sh_ql.get_associated_record_types_query,
                                                **{
                                                    "logs": logs
                                                })
            return result[0]["labels"]

    @Performance.track("node_constructor")
    def _create_node_by_record(self, node_constructor: NodeConstructor, logs: Optional[List[str]]):

        merge_first = node_constructor.get_merge()

        self.connection.exec_query(sh_ql.get_create_node_by_record_constructor_query,
                                   **{
                                       "node_constructor": node_constructor,
                                       "merge": merge_first,
                                       "logs": logs
                                   })

    def create_nodes_by_relations(self, node_types: Optional[List[str]]) -> None:
        for node_constructors in self.semantic_header.get_nodes_constructed_by_relations(node_types).values():
            for node_constructor in node_constructors:
                self._create_node_by_relation(node_constructor=node_constructor)

    @Performance.track("node_constructor")
    def _create_node_by_relation(self, node_constructor: NodeConstructor):
        pass

    def create_relations_using_records(self, relation_types: Optional[List[str]],
                                       logs: Optional[List[str]] = None) -> None:
        # find events that are related to different entities of which one event also has a reference to the other entity
        # create a relation between these two entities
        relation: ConstructedRelation

        # request all associated record labels of the imported logs
        associated_records = self.get_associated_records(logs)

        for relation_constructor in self.semantic_header.get_relations_constructed_by_record(relation_types):
            # check if node_constructor is subset of associated record labels
            # if so, we need to create nodes for this record
            is_subset = set(relation_constructor.prevalent_record.record_types).issubset(set(associated_records))
            if logs is None or is_subset:
                self._create_relations_using_record(relation_constructor=relation_constructor,
                                                    logs=logs)

    @Performance.track("relation_constructor")
    def _create_relations_using_record(self, relation_constructor, logs: Optional[List[str]] = None):
        self.connection.exec_query(sh_ql.get_create_relation_using_record_query,
                                   **{
                                       "relation_constructor": relation_constructor,
                                       "logs": logs
                                   })
        self._create_corr_from_parents(relation_constructor=relation_constructor)

    def create_relations_using_relations(self, relation_types: Optional[List[str]]) -> None:
        relation: ConstructedRelation
        for relation_constructor in self.semantic_header.get_relations_constructed_by_relations(relation_types):
            self._create_relations_using_relation(relation_constructor=relation_constructor)

    def create_relations_using_nodes(self, relation_types: Optional[List[str]]) -> None:
        for relation_constructor in self.semantic_header.get_relations_constructed_by_nodes(relation_types):
            self._create_relations_using_nodes(relation_constructor=relation_constructor)

    @Performance.track("relation_constructor")
    def _create_relations_using_relation(self, relation_constructor):
        self.connection.exec_query(sh_ql.get_create_relation_by_relations_query,
                                   **{
                                       "relation_constructor": relation_constructor
                                   })
        self._create_corr_from_parents(relation_constructor=relation_constructor)

    @Performance.track("relation_constructor")
    def _create_relations_using_nodes(self, relation_constructor):
        self.connection.exec_query(sh_ql.get_create_relation_by_nodes_query,
                                   **{
                                       "relation_constructor": relation_constructor
                                   })
        self._create_corr_from_parents(relation_constructor=relation_constructor)

    def _create_corr_from_parents(self, relation_constructor):
        if relation_constructor.infer_corr_from_reified_parents:
            for use_from in [True, False]:
                self.connection.exec_query(sh_ql.get_infer_corr_from_parent_query,
                                           **{
                                               "relation_constructor": relation_constructor,
                                               "use_from": use_from
                                           })

    def create_df_edges(self, entity_types: List[str], event_label: str, add_duration: bool = False) -> None:
        """
        Create Directly Follows edges for the given entity types using the semantic header information
        :param entity_types: list of entity types for which DF edge should be created. If None, we try for all entities
        :param event_label: the labels of the event nodes for which we want to create the event_label
        :param add_duration: boolean indicating whether we want to add the duration to the df edges
        :return: None
        """
        entity: ConstructedNodes

        if entity_types is None:
            entity_types = [entity.type for entity in self.semantic_header.nodes] \
                           + [relation.type for relation in self.semantic_header.relations if relation.model_as_node]

        for entity in self.semantic_header.nodes:
            if entity.infer_df and entity.type in entity_types:
                self._create_df_edges_for_entity(entity=entity, event_label=event_label, add_duration=add_duration)

        for relation in self.semantic_header.relations:
            if relation.model_as_node and relation.infer_df and relation.type in entity_types:
                self._create_df_edges_for_entity(entity=relation, event_label=event_label, add_duration=add_duration)

    @Performance.track("entity")
    def _create_df_edges_for_entity(self, entity: Union[ConstructedNodes, ConstructedRelation], event_label,
                                    add_duration):
        self.connection.exec_query(sh_ql.get_create_directly_follows_query,
                                   **{
                                       "entity": entity,
                                       "event_label": event_label,
                                       "add_duration": add_duration
                                   })

    def merge_duplicate_df(self):
        node: ConstructedNodes
        for node in self.semantic_header.nodes:
            if node.merge_duplicate_df:
                self._merge_duplicate_df_for_node(node=node)

    @Performance.track("node")
    def _merge_duplicate_df_for_node(self, node: ConstructedNodes):
        self.connection.exec_query(sh_ql.get_merge_duplicate_df_entity_query, **{"node": node})

    def create_static_nodes_and_relations(self):
        print("No implementation yet")
        pass
