import os
from typing import List, Optional, Dict, Any

import pandas as pd

from ..cypher_queries.exporter_ql import ExporterQueryLibrary as exporter_ql
from ..data_managers.semantic_header import ConstructedNodes, SemanticHeader
from ..database_managers.db_connection import DatabaseConnection


class Exporter:
    def __init__(self, db_connection):
        self.connection = db_connection

    def get_event_log(self, entity_type, additional_event_attributes: Optional[List[str]] = None) -> List[
        Dict[str, Any]]:
        """
        Get an event log extracted from the EKG for a specific entity and return it

        Args:
            entity_type: The type of the entity --> contract states that it exists
            additional_event_attributes: list of different attributes of event that should also be stored in the
            event log

        Returns:
            A list of events with its attributes in the form of a dictionary

        Raises:
             ValueError: when the entity has not been defined
        """

        entity_object = SemanticHeader().get_entity(entity_type)
        if entity_object is None:
            raise ValueError(f"Entity {entity_type} is not defined in the semantic header")

        return self._get_event_log(entity_object, additional_event_attributes)

    def _get_event_log(self, entity: ConstructedNodes, additional_event_attributes: Optional[List[str]] = None) -> \
            Optional[List[Dict[str, Any]]]:
        """
        Get an event log extracted from the EKG for a specific entity and return it

        Args:
            entity: The entity for which we want to extract an event log
            additional_event_attributes: list of different attributes of event that should also be stored in the
            event log

        Returns:
            A list of events with its attributes in the form of a dictionary
        """
        if additional_event_attributes is None:
            additional_event_attributes = []
        return self.connection.exec_query(exporter_ql.get_event_log_query,
                                          **{
                                              "entity": entity,
                                              "additional_event_attributes": additional_event_attributes
                                          })

    def save_event_log(self, entity_type: str, additional_event_attributes: Optional[List[str]] = None) -> None:
        """
        Create an event log extracted from the EKG from a specific entity and store it as a csv file

        Args:
            entity_type: The type of the entity
             additional_event_attributes: list of different attributes of event that should also be stored in the
             event log
        """

        event_log = self.get_event_log(entity_type, additional_event_attributes)
        df = pd.DataFrame(event_log)

        current_file_path = os.path.dirname(__file__)

        dir_path = os.path.join(current_file_path, '../../..', '..', 'data', SemanticHeader().name, 'event_logs')
        file_path = os.path.join(dir_path, f"{entity_type}.csv")
        os.makedirs(dir_path, exist_ok=True)
        df.to_csv(file_path, index=True, index_label="idx")
