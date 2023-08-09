from ..cypher_queries.inference_engine_ql import InferenceEngineQueryLibrary as ie_ql
from ..utilities.performance_handling import Performance
from ..database_managers.db_connection import DatabaseConnection


class InferenceEngine:
    def __init__(self, db_connection: DatabaseConnection):
        self.connection = db_connection

    @Performance.track("entity")
    def infer_items_propagate_upwards_multiple_levels(self, entity, is_load=True):
        self.connection.exec_query(ie_ql.get_query_infer_items_propagate_upwards_multiple_levels,
                                   **{
                                       "entity": entity,
                                       "is_load": is_load
                                   })

    @Performance.track("entity")
    def match_entity_with_batch_position(self, entity, relative_position):
        self.connection.exec_query(ie_ql.get_match_entity_with_batch_position_query, **{
            "entity": entity,
            "relative_position": relative_position
        })

    @Performance.track("entity")
    def infer_items_propagate_downwards_multiple_level_w_batching(self, entity, relative_position):
        self.connection.exec_query(ie_ql.get_query_infer_items_propagate_downwards_multiple_level_w_batching,
                                   **{"entity": entity, "relative_position": relative_position})

    @Performance.track("entity")
    def infer_items_propagate_downwards_one_level(self, entity):
        self.connection.exec_query(ie_ql.get_query_infer_items_propagate_downwards_one_level, **{"entity": entity})
