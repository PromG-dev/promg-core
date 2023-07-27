from typing import Dict, Optional, List

from ..data_managers.datastructures import DataStructure
from ..data_managers.semantic_header import RecordConstructor
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
    def get_make_timestamp_date_query(label, attribute, datetime_object, batch_size) -> Query:
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
                'MATCH (e:$label) WHERE e.$attribute IS NOT NULL AND e.justImported = True 
                WITH e, e.$offset as timezone_dt
                WITH e, datetime(apoc.date.convertFormat(timezone_dt, "$datetime_object_format", 
                    "$datetime_object_convert_to")) as converted
                RETURN e, converted',
                'SET e.$attribute = converted',
                {batchSize:$batch_size, parallel:false})
            '''

        return Query(query_str=query_str,
                     template_string_parameters={
                         "label": label,
                         "datetime_object_format": datetime_object.format,
                         "datetime_object_convert_to": datetime_object.convert_to,
                         "batch_size": batch_size,
                         "attribute": attribute,
                         "offset": offset
                     })

    @staticmethod
    def get_convert_epoch_to_timestamp_query(label, attribute, datetime_object, batch_size) -> Query:
        # language=SQL
        query_str = '''
                CALL apoc.periodic.iterate(
                'MATCH (e:$label) WHERE e.$attribute IS NOT NULL AND e.justImported = True 
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
                         "label": label,
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
    def get_create_record_query(record_constructor: RecordConstructor, batch_size: int = 5000):
        # language=SQL
        query_str = '''CALL apoc.periodic.commit(
                            'MATCH ($record) 
                                WHERE record.added_label IS NULL
                                AND $required_attributes_not_null
                                WITH $record_name limit $limit
                                SET record:$labels
                                SET record.added_label = True
                                RETURN count(*)',
                            {limit: $limit})
                        '''

        record_name = "record"
        return Query(query_str=query_str,
                     template_string_parameters={
                         "record": record_constructor.get_prevalent_record_pattern(record_name=record_name),
                         "record_name": "record",
                         "required_attributes_not_null": record_constructor.get_required_attributes_is_not_null_pattern(
                             record_name=record_name),
                         "labels": record_constructor.get_record_labels_pattern()
                     },
                     parameters={"limit": batch_size})

    @staticmethod
    def get_reset_added_label_query(record_constructor: RecordConstructor, batch_size: int):
        query_str = '''
                            CALL apoc.periodic.commit(
                                'MATCH ($record) 
                                    WHERE record.added_label = True
                                    WITH record limit $limit
                                    SET record.added_label = Null
                                    RETURN count(*)',
                                    {limit: $limit})
                            '''
        return Query(query_str=query_str,
                     template_string_parameters={
                         "record": record_constructor.get_prevalent_record_pattern(record_name="record"),
                     },
                     parameters={"limit": batch_size * 10})

    @staticmethod
    def get_mark_records_as_done_query(batch_size: int):
        query_str = '''
                            CALL apoc.periodic.commit(
                                'MATCH (record:Record) 
                                 WITH record limit $limit
                                 remove record:Record
                                 RETURN count(*)',
                                 {limit: $limit})
                            '''
        return Query(query_str=query_str,
                     parameters={"limit": batch_size * 10})
