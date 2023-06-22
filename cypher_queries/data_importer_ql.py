from typing import Dict, Optional, List

from ..data_managers.datastructures import DataStructure
from ..database_managers.db_connection import Query


class DataImporterQueryLibrary:
    @staticmethod
    def get_create_nodes_by_importing_batch_query(batch: List[Dict[str, str]], labels: List[str]) -> Query:
        """
        Create event nodes for each row in the batch with labels
        The properties of each row are also the property of the node
        @param batch: List[Dictionary[key: value]], the key and its values form properties of the event nodes
        @param labels: The labels of the event nodes
        @return: None,
        """

        # $batch is a variable we can add in tx.run, this allows us to use string properties
        # (keys in our dictionary are string)
        # return is required when using call and yield

        # language=SQL
        query_str = '''
                    UNWIND $batch AS row
                    CALL apoc.create.node($labels, row) YIELD node
                    RETURN count(*)
                '''

        return Query(query_str=query_str, parameters={"labels": labels, "batch": batch})

    @staticmethod
    def get_make_timestamp_date_query(attribute, datetime_object, batch_size) -> Query:
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
                'MATCH (e:Event) WHERE e.$attribute IS NOT NULL AND e.justImported = True 
                WITH e, e.$offset as timezone_dt
                WITH e, datetime(apoc.date.convertFormat(timezone_dt, "$datetime_object_format", 
                    "$datetime_object_convert_to")) as converted
                RETURN e, converted',
                'SET e.$attribute = converted',
                {batchSize:$batch_size, parallel:false})
            '''

        return Query(query_str=query_str,
                     template_string_parameters={
                         "datetime_object_format": datetime_object.format,
                         "datetime_object_convert_to": datetime_object.convert_to,
                         "batch_size": batch_size,
                         "attribute": attribute,
                         "offset": offset
                     })

    @staticmethod
    def get_convert_epoch_to_timestamp_query(attribute, datetime_object, batch_size) -> Query:
        # language=SQL
        query_str = '''
                CALL apoc.periodic.iterate(
                'MATCH (e:Event) WHERE e.$attribute IS NOT NULL AND e.justImported = True 
                WITH e, e.$attribute as timezone_dt
                WITH e, apoc.date.format(timezone_dt, $unit, 
                    $dt_format) as converted
                RETURN e, converted',
                'SET e.$attribute = converted',
                {batchSize:$batch_size, parallel:false, 
                params: {unit: $unit,
                        dt_format: $datetime_object_format}})
            '''

        return Query(query_str=query_str,
                     template_string_parameters={"attribute": attribute},
                     parameters={
                         "batch_size": batch_size,
                         "unit": datetime_object.unit,
                         "datetime_object_format": datetime_object.format
                     })

    @staticmethod
    def get_finalize_import_events_query(labels, batch_size) -> Query:
        labels = ":".join(labels)
        # language=SQL
        query_str = '''
            CALL apoc.periodic.iterate(
                'MATCH (e:$labels) 
                WHERE e.justImported = True 
                RETURN e',
                'REMOVE e.justImported',
                {batchSize:$batch_size, parallel:false})
            '''

        return Query(query_str=query_str,
                     template_string_parameters={"labels": labels},
                     parameters={"batch_size": batch_size})

    @staticmethod
    def get_filter_events_by_property_query(prop: str, values: Optional[List[str]] = None, exclude=True) -> Query:
        if values is None:  # match all events that have a specific property
            negation = "NOT" if exclude else ""
            # query to delete all events and its relationship with property
            # language=SQL
            query_str = '''
                    MATCH (e:Event {justImported: True})
                    WHERE e.$prop IS $negation NULL
                    DETACH DELETE e
                    '''
            template_string_parameters = {"prop": prop, "negation": negation}
        else:  # match all events with specific property and value
            negation = "" if exclude else "NOT"
            # match all e and delete them and its relationship
            # language=SQL
            query_str = '''
                    MATCH (e:Event {justImported: true})
                    WHERE $negation e.$prop IN $values
                    DETACH DELETE e
                '''
            template_string_parameters = {"prop": prop, "negation": negation, "values": values}

        # execute query
        return Query(query_str=query_str,
                     template_string_parameters=template_string_parameters)

    @staticmethod
    def get_merge_same_nodes_query(data_structure: DataStructure) -> Query:
        # language=sql
        query_str = '''
                MATCH (n:$labels)
                WITH $primary_keys, collect(n) as nodes
                CALL apoc.refactor.mergeNodes(nodes, {
                    properties:'combine'})
                YIELD node
                RETURN node
            '''

        return Query(query_str=query_str,
                     template_string_parameters={
                         "labels": data_structure.get_label_string(),
                         "primary_keys": data_structure.get_primary_keys_as_attributes()
                     })
