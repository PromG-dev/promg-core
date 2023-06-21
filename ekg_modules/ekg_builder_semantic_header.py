from typing import Optional, List

from ..data_managers.semantic_header import ConstructedNodes, Relation, Relationship, SemanticHeader
from ..database_managers.db_connection import DatabaseConnection
from ..utilities.performance_handling import Performance
from ..cypher_queries.query_library import CypherQueryLibrary


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

    def create_log(self):
        if self.semantic_header.log.include:
            self.connection.exec_query(CypherQueryLibrary.get_create_log_query)
            self._write_message_to_performance(message="Creation of (:Log) nodes")

            if self.semantic_header.log.has:
                self.connection.exec_query(CypherQueryLibrary.get_link_events_to_log_query,
                                           **{"batch_size": self.batch_size})
                self._write_message_to_performance(message="Creation of (:Event) <- [:HAS] - (:Log) relation")

    def create_nodes_by_records(self, node_types: Optional[List[str]]) -> None:
        for node_constructor in self.semantic_header.get_node_by_record_constructors(node_types):
            self.connection.exec_query(CypherQueryLibrary.get_create_node_by_record_constructor_query,
                                       **{"node_constructor": node_constructor})
            self._write_message_to_performance(f"Node ({node_constructor.get_pattern(with_properties=False)}) created")

    def create_nodes_by_relations(self, node_types: Optional[List[str]]) -> None:
        for node_constructor in self.semantic_header.get_nodes_constructed_by_relations(node_types):
            self.connection.exec_query(CypherQueryLibrary.get_create_entities_by_relations_query,
                                       **{"node_constructor": node_constructor})
            self._write_message_to_performance(
                message=f"Relation [{node_constructor.relation.get_pattern()}] reified as "
                        f"({node_constructor.get_pattern(with_properties=False)}) node")

    def create_entity_relations_using_nodes(self, relation_types: Optional[List[str]]) -> None:
        # find events that are related to different entities of which one event also has a reference to the other entity
        # create a relation between these two entities
        if relation_types is None:
            relation_types = [relation.type for relation in self.semantic_header.get_relations_derived_from_nodes()]

        relation: Relation
        for relation in self.semantic_header.get_relations_derived_from_nodes():
            if relation.include and relation.type in relation_types:
                self.connection.exec_query(CypherQueryLibrary.get_create_relation_using_nodes_query,
                                           **{"relation": relation})
                self._write_message_to_performance(
                    message=f"Relation {relation.result.get_pattern(exclude_nodes=False)} done")

    def create_entity_relations_using_relations(self, relation_types: Optional[List[str]]) -> None:
        if relation_types is None:
            relation_types = [relation.type for relation in self.semantic_header.get_relations_derived_from_relations()]
        relation: Relation
        for relation in self.semantic_header.get_relations_derived_from_relations():
            if relation.include and relation.type in relation_types:
                self.connection.exec_query(CypherQueryLibrary.get_create_relation_by_relations_query,
                                           **{"relation": relation,
                                              "batch_size": self.batch_size})

    def create_df_edges(self, entity_types) -> None:
        entity: ConstructedNodes

        if entity_types is None:
            entity_types = [entity.node_type for entity in self.semantic_header.nodes]

        for entity in self.semantic_header.nodes:
            if entity.infer_df and entity.node_type in entity_types:
                self.connection.exec_query(CypherQueryLibrary.get_create_directly_follows_query,
                                           **{"entity": entity, "batch_size": self.batch_size})
                self._write_message_to_performance(f"Created [:DF] edge for (:{entity.get_label_string()})")

    def merge_duplicate_df(self):
        entity: ConstructedNodes
        for entity in self.semantic_header.nodes:
            if entity.merge_duplicate_df:
                self.connection.exec_query(CypherQueryLibrary.get_merge_duplicate_df_entity_query, **{"entity": entity})
                self.perf.finished_step(
                    activity=f"Merged duplicate [:DF] edges for (:{entity.get_label_string()}) done")

    def delete_parallel_dfs_derived(self):
        reified_entity: ConstructedNodes
        original_entity: ConstructedNodes
        relation: Relationship
        for reified_entity in self.semantic_header.get_nodes_constructed_by_relations():
            if reified_entity.delete_parallel_df:
                relation = reified_entity.node_constructors.relation
                parent_entity = self.semantic_header.get_entity(relation.from_node.node_label)
                child_entity = self.semantic_header.get_entity(relation.to_node.node_label)
                for original_entity in [parent_entity, child_entity]:
                    self.connection.exec_query(CypherQueryLibrary.delete_parallel_directly_follows_derived,
                                               **{"reified_entity": reified_entity,
                                                  "original_entity": original_entity})
                    self._write_message_to_performance(
                        f"Deleted parallel DF of (:{reified_entity.get_label_string()}) and (:{original_entity.get_label_string()})")

    def create_static_nodes_and_relations(self):
        self._write_message_to_performance("No implementation yet")

    def add_attributes_to_classifier(self, relation, label, properties, copy_as):
        self.connection.exec_query(CypherQueryLibrary.add_attributes_to_classifier,
                                   **{"relation": relation, "label": label, "properties": properties,
                                      "copy_as": copy_as})
