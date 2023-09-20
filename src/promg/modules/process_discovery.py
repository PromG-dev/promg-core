from .. import SemanticHeader
from ..database_managers.db_connection import DatabaseConnection
from ..utilities.performance_handling import Performance
from ..cypher_queries.process_discovery_ql import AnalysisQueryLibrary as analysis_ql


class ProcessDiscovery:
    """
        Create ProcessDiscovery module
        Examples:
            >>> from promg.modules.process_discovery import ProcessDiscovery
            >>> process_discovery = ProcessDiscovery()
    """
    def __init__(self):
        self.connection = DatabaseConnection()

    def create_df_process_model(self, entity_type: str):
        """
        Create a DF process model

        Args:
            entity_type: The type of the entity

        Raises:
            ValueError: when the entity has not been defined

        Examples:
            >>> process_discovery.create_df_process_model(entity_type="Book")
            A process model is created from the (:Book) perspective
        """
        entity = SemanticHeader().get_entity(entity_type)
        if entity_type is None:
            raise ValueError(f"{entity_type} is not defined in semantic header")
        self._create_df_process_model(entity=entity)

    @Performance.track("entity")
    def _create_df_process_model(self, entity):
        self.connection.exec_query(analysis_ql.get_aggregate_df_relations_query,
                                   **{"entity": entity})
