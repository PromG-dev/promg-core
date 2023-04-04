from typing import List, Set, Optional, Dict


from ..database_managers.db_connection import DatabaseConnection
from ..utilities.performance_handling import Performance
from ..cypher_queries.query_library import CypherQueryLibrary as cql


class EKGAnalysis:
    def __init__(self, db_connection: DatabaseConnection, perf: Performance):
        self.connection = db_connection
        self.perf = perf

    def _write_message_to_performance(self, message: str):
        if self.perf is not None:
            self.perf.finished_step(activity=message)

    def create_df_process_model(self, entity, classifiers):
        self.connection.exec_query(cql.get_aggregate_df_relations_query,
                                   **{"entity": entity, "classifiers": classifiers})
