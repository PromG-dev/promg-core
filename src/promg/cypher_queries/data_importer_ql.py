from typing import Dict, Optional, List, Union
from string import Template

from ..data_managers.datastructures import DataStructure
from ..data_managers.semantic_header import RecordConstructor
from ..database_managers.db_connection import Query


class DataImporterQueryLibrary:

    @staticmethod
    def determine_mapping_str(mapping):
        if mapping == "":
            return ""
        mapping_str = ''',{nullValues: [""], mapping:$mapping}''' if mapping != "" else ""
        mapping_str = Template(mapping_str).safe_substitute({"mapping": mapping})
        return mapping_str

    @staticmethod
    def get_create_nodes_by_loading_csv_query(labels: str, file_name: str, mapping: str) -> Query:
        """
        Create event nodes for each row in the batch with labels
        The properties of each row are also the property of the node
        @param mapping:
        @param file_name:
        @param labels: The labels of the record nodes

        @return: None
        """

        # $batch is a variable we can add in tx.run, this allows us to use string properties
        # (keys in our dictionary are string)
        # return is required when using call and yield
        # language=SQL
        query_str = '''
                    CALL apoc.periodic.iterate('
                        CALL apoc.load.csv("$file_name" $mapping_str) yield map as row return row',
                        'CREATE (record:$labels)
                        SET record += row'
                    , {batchSize:10000, parallel:true, retries: 1});                    
                '''

        return Query(query_str=query_str,
                     template_string_parameters={
                         "file_name": file_name,
                         "labels": labels,
                         "mapping_str": DataImporterQueryLibrary.determine_mapping_str(mapping)
                     })

    @staticmethod
    def get_make_timestamp_date_query(required_labels_str, attribute, datetime_object, load_status) -> Query:
        """
        Convert the strings of the timestamp to the datetime as used in cypher
        Remove the str_timestamp property
        @return: None
        """
        offset = datetime_object.timezone_offset
        offset = f'{attribute}+"{offset}"' if offset != "" else attribute

        # language=SQL
        query_str = '''
                CALL apoc.periodic.iterate(
                'MATCH (r:$required_labels) 
                USING INDEX r:Record(loadStatus)
                WHERE r.$attribute IS NOT NULL AND r.loadStatus = $load_status 
                AND NOT apoc.meta.cypher.isType(r.$attribute, "$date_type")
                WITH r, r.$offset as timezone_dt
                WITH r, datetime(apoc.date.convertFormat(timezone_dt, "$datetime_object_format", 
                    "$datetime_object_convert_to")) as converted
                RETURN r, converted',
                'SET r.$attribute = converted',
                {batchSize:$batch_size, parallel:true})
            '''

        return Query(query_str=query_str,
                     template_string_parameters={
                         "required_labels": required_labels_str,
                         "datetime_object_format": datetime_object.format,
                         "datetime_object_convert_to": datetime_object.convert_to,
                         "date_type": datetime_object.get_date_type(),
                         "attribute": attribute,
                         "offset": offset,
                         "load_status": load_status
                     })

    @staticmethod
    def get_convert_epoch_to_timestamp_query(required_labels_str, attribute, datetime_object, load_status) -> Query:
        # language=SQL
        query_str = '''
                CALL apoc.periodic.iterate(
                'MATCH (r:$required_labels) 
                USING INDEX r:Record(loadStatus)
                WHERE r.$attribute IS NOT NULL AND r.loadStatus = $load_status 
                WITH r, r.$attribute as timezone_dt
                WITH r, apoc.date.format(timezone_dt, $unit, 
                    $dt_format) as converted
                RETURN r, converted',
                'SET r.$attribute = converted',
                {batchSize:$batch_size, parallel:false, 
                params: {unit: $unit,
                        dt_format: $datetime_object_format}})
            '''

        return Query(query_str=query_str,
                     template_string_parameters={
                         "attribute": attribute,
                         "required_labels": required_labels_str
                     },
                     parameters={
                         "unit": datetime_object.unit,
                         "datetime_object_format": datetime_object.format,
                         "load_status": load_status
                     })

    @staticmethod
    def get_finalize_import_records_query(load_status, required_labels) -> Query:
        # language=SQL
        query_str = '''
            CALL apoc.periodic.commit(
                'MATCH (r:$required_labels) 
                USING INDEX r:Record(loadStatus)
                WHERE r.loadStatus = $load_status
                WITH r LIMIT $limit
                REMOVE r.loadStatus
                RETURN count(*)',
                {limit:$batch_size, load_status:$load_status})
            '''

        return Query(query_str=query_str,
                     template_string_parameters={"required_labels": required_labels},
                     parameters={
                         "load_status": load_status
                     }
                     )

    @staticmethod
    def get_filter_records_by_property_query(prop: str, load_status: int, values: Optional[List[str]] = None,
                                             exclude=True, required_labels="Record") -> Query:
        if values is None:  # match all events that have a specific property
            negation = "NOT" if exclude else ""
            # query to delete all events and its relationship with property
            # language=SQL
            query_str = '''
                    MATCH (r:$required_labels {loadStatus: $load_status})
                    WHERE e.$prop IS $negation NULL
                    DETACH DELETE e
                    '''
            template_string_parameters = {"prop": prop, "negation": negation}
        else:  # match all events with specific property and value
            negation = "" if exclude else "NOT"
            # match all e and delete them and its relationship
            # language=SQL
            query_str = '''
                    MATCH (r:$required_labels {loadStatus: $load_status})
                    WHERE $negation e.$prop IN $values
                    DETACH DELETE e
                '''
            template_string_parameters = {
                "prop": prop,
                "negation": negation,
                "values": values,
                "load_status": load_status,
                "required_labels": required_labels
            }

        # execute query
        return Query(query_str=query_str,
                     template_string_parameters=template_string_parameters)

    @staticmethod
    def get_update_load_status_query(current_load_status: int):
        query_str = '''
        CALL apoc.periodic.commit(
            'MATCH (record:Record) 
            WHERE record.loadStatus = $old_value
            WITH record LIMIT $limit
            SET record.loadStatus = record.loadStatus + 1
            RETURN COUNT(*)',
            {old_value: $old_value,
            limit: $batch_size})
                            '''
        return Query(query_str=query_str,
                     parameters={
                         "old_value": current_load_status
                     })
