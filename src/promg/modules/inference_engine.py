from .. import SemanticHeader
from ..cypher_queries.inference_engine_ql import InferenceEngineQueryLibrary as ie_ql
from ..data_managers.semantic_header import ConstructedNodes
from ..utilities.performance_handling import Performance
from ..database_managers.db_connection import DatabaseConnection


class InferenceEngine:
    def __init__(self, db_connection):
        self.connection = db_connection

    def match_entity_with_batch_position(self, entity_type: str, relative_position_type: str) -> None:
        """
        Infer the batch position of a specific entity

        Args:
            entity_type: The type of the entity
             relative_position_type: The type of the relative position

        Raises:
             ValueError: when the entity has not been defined
        """
        entity = SemanticHeader().get_entity(entity_type)
        relative_position = SemanticHeader().get_entity(relative_position_type)
        if entity is None:
            raise ValueError(f"{entity_type} is not defined in semantic header")
        if relative_position is None:
            raise ValueError(f"{relative_position} is not defined in semantic header")
        self._match_entity_with_batch_position(entity, relative_position)

    @Performance.track("entity")
    def _match_entity_with_batch_position(self, entity: ConstructedNodes, relative_position: ConstructedNodes):
        self.connection.exec_query(ie_ql.get_match_entity_with_batch_position_query, **{
            "entity": entity,
            "relative_position": relative_position
        })

    # rule B
    def infer_items_propagate_downwards_one_level(self, entity_type: str) -> None:
        """
        Infer items while propagating downwards one level

        Args:
            entity_type: The type of the entity

        Raises:
             ValueError: when the entity has not been defined
        """
        entity = SemanticHeader().get_entity(entity_type)
        if entity_type is None:
            raise ValueError(f"{entity_type} is not defined in semantic header")
        self._infer_items_propagate_downwards_one_level(entity)

    @Performance.track("entity")
    def _infer_items_propagate_downwards_one_level(self, entity: ConstructedNodes):
        self.connection.exec_query(ie_ql.get_query_infer_items_propagate_downwards_one_level, **{"entity": entity})

    # rule C
    def infer_items_propagate_upwards_multiple_levels(self, entity_type: str, is_load=True) -> None:
        """
        Infer items while propagating upwards multiple levels (Rule C)

        Args:
            entity_type: The type of the entity
            is_load: indicating whether we are inferring upwards to load events (true) or unload events (false)

        Raises:
            ValueError: when the entity has not been defined
        """

        entity = SemanticHeader().get_entity(entity_type)
        if entity is None:
            raise ValueError(f"{entity_type} is not defined in semantic header")
        self._infer_items_propagate_upwards_multiple_levels(entity, is_load)

    @Performance.track("entity")
    def _infer_items_propagate_upwards_multiple_levels(self, entity, is_load=True):
        self.connection.exec_query(ie_ql.get_query_infer_items_propagate_upwards_multiple_levels,
                                   **{
                                       "entity": entity,
                                       "is_load": is_load
                                   })

    # rule D
    def infer_items_propagate_downwards_multiple_level_w_batching(self, entity_type: str,
                                                                  relative_position_type: str, ) -> None:
        """
        Infer items while propagating downwards multiple levels with batching (rule D)

        Args:
            entity_type: The type of the entity
            relative_position_type: The type of the relative position

        Raises:
            ValueError: when the entity has not been defined in semantic header
        """
        entity = SemanticHeader().get_entity(entity_type)
        relative_position = SemanticHeader().get_entity(relative_position_type)
        if entity_type is None:
            raise ValueError(f"{entity_type} is not defined in semantic header")
        self._infer_items_propagate_downwards_multiple_level_w_batching(entity, relative_position)

    @Performance.track("entity")
    def _infer_items_propagate_downwards_multiple_level_w_batching(self, entity, relative_position):
        self.connection.exec_query(ie_ql.get_query_infer_items_propagate_downwards_multiple_level_w_batching,
                                   **{"entity": entity, "relative_position": relative_position})
