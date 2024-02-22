from ..cypher_queries.task_identification_ql import TaskIdentifierLibrary as tf_ql
from ..data_managers.semantic_header import ConstructedNodes, SemanticHeader
from ..utilities.performance_handling import Performance
from ..database_managers.db_connection import DatabaseConnection


class TaskIdentification:
    def __init__(self, db_connection, semantic_header, resource: str, case: str):
        self.connection = db_connection
        self.resource: ConstructedNodes = semantic_header.get_entity(resource)
        self.case: ConstructedNodes = semantic_header.get_entity(case)

    @Performance.track("resource")
    def identify_tasks(self):
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
