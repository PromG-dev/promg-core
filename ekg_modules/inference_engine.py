from cypher_queries.query_library import CypherQueryLibrary as cql


class InferenceEngine:
    def __init__(self, db_connection, perf):
        self.connection = db_connection
        self.perf = perf

    def _write_message_to_performance(self, message: str):
        if self.perf is not None:
            self.perf.finished_step(activity=message)

    def infer_items_propagate_upwards_multiple_levels(self, entity, is_load=True):
        self.connection.exec_query(cql.get_query_infer_items_propagate_upwards_multiple_levels,
                                   **{"entity": entity,
                                      "is_load": is_load})
        self._write_message_to_performance("Batch items are inferred")

    def match_entity_with_batch_position(self, entity):
        self.connection.exec_query(cql.match_entity_with_batch_position, **{"entity": entity})
        self._write_message_to_performance("Entities are matched with batch position")

    def infer_items_propagate_downwards_multiple_level_w_batching(self, entity):
        self.connection.exec_query(cql.get_query_infer_items_propagate_downwards_multiple_level_w_batching,
                                   **{"entity": entity})

    def infer_items_propagate_downwards_one_level(self, entity):
        self.connection.exec_query(cql.get_query_infer_items_propagate_downwards_one_level, **{"entity": entity})

    def add_entity_to_event(self, entity):
        self.connection.exec_query(cql.add_entity_to_event, **{"entity": entity})
