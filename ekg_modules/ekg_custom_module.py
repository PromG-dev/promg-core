from typing import List, Set, Optional, Dict

from ..database_managers.db_connection import DatabaseConnection
from ..utilities.performance_handling import Performance
from ..cypher_queries.custom_query_library import CustomCypherQueryLibrary as ccql


class CustomModule:
    def __init__(self, db_connection: DatabaseConnection, perf: Performance):
        self.connection = db_connection
        self.perf = perf

    def _write_message_to_performance(self, message: str):
        if self.perf is not None:
            self.perf.finished_step(activity=message)

    def do_custom_query(self, query_name, **kwargs):
        func = getattr(self, query_name)
        func(**kwargs)

    def create_stations(self, entity_type):
        self.connection.exec_query(ccql.get_create_source_station_query,
                                   **{"entity_type": entity_type})

        self.connection.exec_query(ccql.get_create_sink_station_query,
                                   **{"entity_type": entity_type})
        self.connection.exec_query(ccql.get_create_processing_stations_query,
                                   **{"entity_type": entity_type})

    def correlate_events_to_station(self):
        self.connection.exec_query(ccql.get_correlate_events_to_stations_query)
