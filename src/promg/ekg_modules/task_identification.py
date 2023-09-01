from ..cypher_queries.task_identification_ql import TaskIdentifierLibrary as tf_ql
from ..utilities.performance_handling import Performance
from ..database_managers.db_connection import DatabaseConnection


class TaskIdentification:
    def __init__(self):
        self.connection = DatabaseConnection()

    @Performance.track("resource")
    def identify_tasks(self, resource, case):
        entities = [resource, case]
        self.connection.exec_query(tf_ql.get_combine_df_joint_query,
                                   **{"resource": resource,
                                      "case": case})

        self.connection.exec_query(tf_ql.get_create_task_instances_query,
                                   **{"resource": resource})
        self.connection.exec_query(tf_ql.get_split_ti_nodes_create_new_1_query)
        self.connection.exec_query(tf_ql.get_split_ti_nodes_create_new_2_query)
        self.connection.exec_query(tf_ql.get_split_ti_nodes_remove_old_query)
        self.connection.exec_query(tf_ql.get_remove_df_joint_query)
        for entity in entities:
            self.connection.exec_query(tf_ql.get_correlate_ti_to_entity_query,
                                       **{"entity": entity})
            self.connection.exec_query(tf_ql.get_lift_df_to_task_instances_query,
                                       **{"entity": entity})

    @Performance.track("resource")
    def aggregate_on_task_variant(self, resource, case):
        entities = [resource, case]
        self.connection.exec_query(tf_ql.get_aggregate_task_instances_query,
                                   **{"property": "variant"})
        self.connection.exec_query(tf_ql.get_link_task_instances_to_aggregations_query,
                                   **{"property": "variant"})
        for entity in entities:
            self.connection.exec_query(tf_ql.get_lift_df_to_task_aggregations_query,
                                       **{"property": "variant",
                                          "entity": entity})
