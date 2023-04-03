from dataclasses import dataclass
from typing import Dict, Optional

from string import Template


@dataclass
class Query:
    query_string: str
    kwargs: Optional[Dict[str, any]]


class CustomCypherQueryLibrary:

    @staticmethod
    def get_create_source_station_query(entity_type):
        query_str = '''
            MATCH (c_start:Class)
            WHERE NOT EXISTS ((:Class) - [:$df_c_type] -> (c_start))
            WITH c_start, "SourceStation"+c_start.cID as id
            MERGE (c_start) - [:AT] -> (:Entity:Station {entityType: "Station", type: "Source", sensor: c_start.cID,
                                                           ID: id, uID:"Station_"+id})
        '''

        query_str = Template(query_str).substitute(df_c_type=f"DF_C_{entity_type.upper()}")
        return Query(query_string=query_str, kwargs={})

    @staticmethod
    def get_create_sink_station_query(entity_type):
        query_str = '''
                MATCH (c_end:Class)
                WHERE NOT EXISTS ((c_end) - [:$df_c_type] -> (:Class))
                WITH c_end, "SinkStation"+c_end.cID as id
                MERGE (c_end) - [:AT] -> (:Entity:Station {entityType: "Station", type: "Sink", sensor: c_end.cID,
                                                           ID: id, uID:"Station_"+id})
            '''

        query_str = Template(query_str).substitute(df_c_type=f"DF_C_{entity_type.upper()}")
        return Query(query_string=query_str, kwargs={})

    @staticmethod
    def get_create_processing_stations_query(entity_type):
        query_str = '''
                    MATCH p=(c_start:Class) - [:$df_c_type*] -> (c_end:Class)
                    WHERE NOT EXISTS ((c_end) - [:$df_c_type] -> (:Class)) AND NOT EXISTS ((:Class) - [:$df_c_type] -> (c_start))
                    WITH nodes(p) as classList
                    UNWIND range(1,size(classList)-3,2) AS i
                    WITH classList[i] as first, classList[i+1] as second
                    WITH first, second, "ProceccingStation"+first.cID++second.cID as id
                    MERGE (first) - [:AT] -> (:Entity:Station {entityType: "Station", type: "Processing", 
                                                start_sensor: first.cID, end_sensor: second.cID,
                                                ID: id, uID:"Station_"+id}) <- [:AT] - (second)
                '''

        query_str = Template(query_str).substitute(df_c_type=f"DF_C_{entity_type.upper()}")
        return Query(query_string=query_str, kwargs={})

    @staticmethod
    def get_correlate_events_to_stations_query():
        query_str = '''
            MATCH (e:Event) - [:OBSERVED] -> (c:Class) - [:AT] -> (s:Station)
            MERGE (e) - [:CORR] -> (s)
        '''
        return Query(query_string=query_str, kwargs={})
