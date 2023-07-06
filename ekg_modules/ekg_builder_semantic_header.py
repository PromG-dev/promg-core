from typing import Optional, List

from ..data_managers.semantic_header import ConstructedNodes, ConstructedRelation, Relationship, SemanticHeader, \
    NodeConstructor
from ..database_managers.db_connection import DatabaseConnection
from ..utilities.performance_handling import Performance
from ..cypher_queries.semantic_header_ql import SemanticHeaderQueryLibrary as sh_ql


class EKGUsingSemanticHeaderBuilder:
    def __init__(self, db_connection: DatabaseConnection, semantic_header: SemanticHeader, batch_size: int,
                 perf: Performance):
        self.connection = db_connection
        self.semantic_header = semantic_header
        self.batch_size = batch_size
        self.perf = perf

    def _write_message_to_performance(self, message: str):
        if self.perf is not None:
            self.perf.finished_step(activity=message)

    def create_nodes_by_records(self, node_types: Optional[List[str]]) -> None:
        for node_constructor in self.semantic_header.get_node_by_record_constructors(node_types):
            self.connection.exec_query(sh_ql.get_create_node_by_record_constructor_query,
                                       **{
                                           "node_constructor": node_constructor,
                                           "batch_size": self.batch_size
                                       })
            self._write_message_to_performance(f"Node ({node_constructor.get_pattern(with_properties=False)}) created")

            self.connection.exec_query(sh_ql.get_reset_created_record_query,
                                       **{"node_constructor": node_constructor,
                                          "batch_size": self.batch_size}
                                       )

            max_limit = self.connection.exec_query(sh_ql.get_number_of_ids_query,
                                                   **{"node_constructor": node_constructor})
            self.connection.exec_query(sh_ql.get_merge_nodes_with_same_id_query,
                                       **{"node_constructor": node_constructor,
                                          "batch_size": max(self.batch_size*10, max_limit[0]['num_ids']*2)}
                                       )

            self._write_message_to_performance(f"Node ({node_constructor.get_pattern(with_properties=False)}) merged")

    def create_nodes_by_relations(self, node_types: Optional[List[str]]) -> None:
        for node_constructors in self.semantic_header.get_nodes_constructed_by_relations(node_types).values():
            for node_constructor in node_constructors:
                self.connection.exec_query(sh_ql.get_create_nodes_by_relations_query,
                                           **{"node_constructor": node_constructor})
                self._write_message_to_performance(
                    message=f"Relation [{node_constructor.relation.get_pattern()}] reified as "
                            f"({node_constructor.get_pattern(with_properties=False)}) node")

    def create_relations_using_record(self, relation_types: Optional[List[str]]) -> None:
        # find events that are related to different entities of which one event also has a reference to the other entity
        # create a relation between these two entities
        relation: ConstructedRelation
        for relation_constructor in self.semantic_header.get_relations_constructed_by_record(relation_types):
            self.connection.exec_query(sh_ql.get_create_relation_using_record_query,
                                       **{"relation_constructor": relation_constructor})
            self._write_message_to_performance(
                message=f"Relation {relation_constructor.get_pattern()} done")

    def create_relations_using_relations(self, relation_types: Optional[List[str]]) -> None:
        relation: ConstructedRelation
        for relation_constructor in self.semantic_header.get_relations_constructed_by_relations(relation_types):
            self.connection.exec_query(sh_ql.get_create_relation_by_relations_query,
                                       **{
                                           "relation_constructor": relation_constructor,
                                           "batch_size": self.batch_size
                                       })

    def create_df_edges(self, entity_types) -> None:
        entity: ConstructedNodes

        if entity_types is None:
            entity_types = [entity.node_type for entity in self.semantic_header.nodes]

        for entity in self.semantic_header.nodes:
            if entity.infer_df and entity.node_type in entity_types:
                self.connection.exec_query(sh_ql.get_create_directly_follows_query,
                                           **{"entity": entity, "batch_size": self.batch_size})
                self._write_message_to_performance(f"Created [:DF] edge for (:{entity.get_label_string()})")

    def merge_duplicate_df(self):
        node: ConstructedNodes
        for node in self.semantic_header.nodes:
            if node.merge_duplicate_df:
                self.connection.exec_query(sh_ql.get_merge_duplicate_df_entity_query, **{"node": node})
                self.perf.finished_step(
                    activity=f"Merged duplicate [:DF] edges for (:{node.get_label_string()}) done")

    def delete_parallel_dfs_derived(self):
        node: ConstructedNodes
        original_entity: ConstructedNodes
        relation: Relationship
        node_constructor: NodeConstructor
        for _type, node_constructor in self.semantic_header.get_nodes_constructed_by_relations(
                only_include_delete_parallel_df=True).items():
            from_node = node_constructor.relation.from_node
            to_node = node_constructor.relation.to_node
            for node in [from_node, to_node]:
                self.connection.exec_query(sh_ql.delete_parallel_directly_follows_derived,
                                           **{
                                               "type": _type,
                                               "node": node
                                           })
                self._write_message_to_performance(
                    f"Deleted parallel DF of (:{node.get_label_string()}) and (:"
                    f"{type})")

    def create_static_nodes_and_relations(self):
        self._write_message_to_performance("No implementation yet")
