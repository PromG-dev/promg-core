from ..database_managers.db_connection import DatabaseConnection
from ..utilities.performance_handling import Performance
from ..cypher_queries.analysis_ql import AnalysisQueryLibrary as analysis_ql


class EKGAnalysis:
    def __init__(self, db_connection: DatabaseConnection):
        self.connection = db_connection

    @Performance.track("entity")
    def create_df_process_model(self, entity):
        self.connection.exec_query(analysis_ql.get_aggregate_df_relations_query,
                                   **{"entity": entity})
