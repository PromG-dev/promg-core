from ..cypher_queries.task_identification_ql import TaskIdentifierLibrary as tf_ql
from ..utilities.performance_handling import Performance
from ..database_managers.db_connection import DatabaseConnection


class TaskIdentification:
    def __init__(self, db_connection: DatabaseConnection):
        self.connection = db_connection

    @Performance.track("entity")
    def identify_tasks(self):
        self.connection.exec_query(tf_ql.get_task_identifier_query,
                                   **{})

        # self.connection.exec_query(ie_ql.get_query_infer_items_propagate_upwards_multiple_levels,
        #                            **{
        #                                "entity": entity,
        #                                "is_load": is_load
        #                            })
