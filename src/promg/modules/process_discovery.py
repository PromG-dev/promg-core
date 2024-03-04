from .. import SemanticHeader
from ..database_managers.db_connection import DatabaseConnection
from ..utilities.performance_handling import Performance
from ..cypher_queries.process_discovery_ql import AnalysisQueryLibrary as analysis_ql


class ProcessDiscovery:
    def __init__(self, db_connection):
        self.connection = db_connection

    def create_df_process_model(self, entity_type: str):
        """
        Create a DF process model

        Args:
            entity_type: The type of the entity

        Raises:
            ValueError: when the entity has not been defined
        """
        entity = SemanticHeader().get_entity(entity_type)
        if entity_type is None:
            raise ValueError(f"{entity_type} is not defined in semantic header")
        self._create_df_process_model(entity=entity)

    @Performance.track("entity")
    def _create_df_process_model(self, entity):
        self.connection.exec_query(analysis_ql.get_aggregate_df_relations_query,
                                   **{"entity": entity})
