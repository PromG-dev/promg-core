from ..cypher_queries.task_identification_ql import TaskIdentifierLibrary as tf_ql
from ..data_managers.semantic_header import ConstructedNodes, SemanticHeader
from ..utilities.performance_handling import Performance
from ..database_managers.db_connection import DatabaseConnection


class TaskIdentification:
    """
        Create TaskIdentification module
        Examples:
            >>> from promg.modules.task_identification import TaskIdentification
            >>> task_identifier = TaskIdentification(resource="Resource", case="CASE_AWO")
            returns a task_identifier module from the perspective "Resource" and the "CASE_AWO" entities

    """
    def __init__(self, resource: str, case: str):
        self.connection = DatabaseConnection()
        self.resource: ConstructedNodes = SemanticHeader().get_entity(resource)
        self.case: ConstructedNodes = SemanticHeader().get_entity(case)

    @Performance.track()
    def identify_tasks(self):
        """
            Method to create (:TaskInstance) nodes and [:CONTAINS] from (:Event) nodes to (:TaskInstance) nodes

            Examples:
                >>> from promg.modules.task_identification import TaskIdentification
                >>> task_identifier = TaskIdentification(resource="Resource", case="CASE_AWO")
                >>> task_identifier.identify_tasks()
                Identifies and creates (:TaskInstance) nodes for the given resource and case

        """
        self.connection.exec_query(tf_ql.get_combine_df_joint_query,
                                   **{
                                       "resource": self.resource,
                                       "case": self.case
                                   })

        self.connection.exec_query(tf_ql.get_create_task_instances_query,
                                   **{"resource": self.resource})
        self.connection.exec_query(tf_ql.get_split_ti_nodes_create_new_1_query)
        self.connection.exec_query(tf_ql.get_split_ti_nodes_create_new_2_query)
        self.connection.exec_query(tf_ql.get_split_ti_nodes_remove_old_query)
        self.connection.exec_query(tf_ql.get_remove_df_joint_query)
        for entity in [self.resource, self.case]:
            self.connection.exec_query(tf_ql.get_correlate_ti_to_entity_query,
                                       **{"entity": entity})
            self.connection.exec_query(tf_ql.get_lift_df_to_task_instances_query,
                                       **{"entity": entity})

    @Performance.track("resource")
    def aggregate_on_task_variant(self):
        """
            Method to aggregate (:TaskInstance) nodes into (:TaskAggregation) nodes

            Examples:
                >>> from promg.modules.task_identification import TaskIdentification
                >>> task_identifier = TaskIdentification(resource="Resource", case="CASE_AWO")
                >>> task_identifier.aggregate_on_task_variant()
                Identifies and creates (:TaskAggegration) given there exists (:TaskInstance) nodes

        """
        self.connection.exec_query(tf_ql.get_aggregate_task_instances_query,
                                   **{"property": "variant"})
        self.connection.exec_query(tf_ql.get_link_task_instances_to_aggregations_query,
                                   **{"property": "variant"})
        for entity in [self.resource, self.case]:
            self.connection.exec_query(tf_ql.get_lift_df_to_task_aggregations_query,
                                       **{
                                           "property": "variant",
                                           "entity": entity
                                       })
