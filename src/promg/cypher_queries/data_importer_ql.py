from typing import Dict, Optional, List, Union
from string import Template

from ..data_managers.datastructures import DataStructure
from ..data_managers.semantic_header import RecordConstructor
from ..database_managers.db_connection import Query


class DataImporterQueryLibrary:
    @staticmethod
    def get_optional_label_str(get_labels_str, optional_labels):
        record_name = "record"
        optional_label_str = ""
        if len(optional_labels) > 0:
            # language=SQL
            optional_label_str = '''
                    $assign_labels_str
                    CALL apoc.create.addLabels($record_name, $optional_labels_list)
                    YIELD node as record_with_labels
                    '''
        optional_label_str = Template(optional_label_str).safe_substitute({
            "record_name": record_name,
            "assign_labels_str": get_labels_str,
            "optional_labels_list": "+".join(optional_labels)
        })

        return optional_label_str

    @staticmethod
    def get_label_constructors(record_constructors: List[Dict[bool, RecordConstructor]]):
        required_labels = ['Record']
        optional_labels = []
        record_name = "record"
        get_labels_str = ""
        for index, record_constructor_dict in enumerate(record_constructors):
            record_constructor = record_constructor_dict["record_constructor"]
            if record_constructor_dict["required"]:
                required_labels.extend(record_constructor.record_labels)
            else:
                with_str = '''
                            CALL apoc.when($required_attributes_not_null $attributes_match, 'RETURN $labels as 
                            labels', 'RETURN [] as labels')
                            YIELD value as labels$i'''
                with_str = Template(with_str).safe_substitute({
                    "attributes_match": record_constructor.get_additional_conditions(),
                    "required_attributes_not_null": record_constructor.get_required_attributes_is_not_null_pattern(
                        record_name=record_name),
                    "labels": record_constructor.get_label_list(as_str=True),
                    "record_name": record_name,
                    "i": index
                })
                get_labels_str += with_str
                optional_labels.append(f"labels{index}['labels']")

        optional_labels_str = DataImporterQueryLibrary.get_optional_label_str(optional_labels=optional_labels,
                                                                              get_labels_str=get_labels_str)

        label_constructor = {
            "required_labels": required_labels,
            "optional_labels_str": optional_labels_str
        }

        return label_constructor

    @staticmethod
    def get_create_nodes_by_importing_batch_query(batch: List[Dict[str, str]],
                                                  labels_constructors: Dict[str, Union[List[str], str]]) -> Query:
        """
        Create event nodes for each row in the batch with labels
        The properties of each row are also the property of the node
        @param batch: List[Dictionary[key: value]], the key and its values form properties of the event nodes
        @param labels: The labels of the event nodes
        @return: None,
        """

        record_name = "record"
        required_labels = labels_constructors["required_labels"]
        optional_labels_str = labels_constructors["optional_labels_str"]

        # $batch is a variable we can add in tx.run, this allows us to use string properties
        # (keys in our dictionary are string)
        # return is required when using call and yield
        # language=SQL
        query_str = '''
                    UNWIND $batch AS row
                    CALL apoc.create.node($required_labels, row) YIELD node AS $record_name
                    $optional_label_str
                    RETURN count(*)
                '''

        return Query(query_str=query_str,
                     template_string_parameters={
                         "record_name": record_name,
                         "required_labels": required_labels,
                         "optional_label_str": optional_labels_str
                     },
                     parameters={"batch": batch})

    @staticmethod
    def get_make_timestamp_date_query(attribute, datetime_object, batch_size, load_status) -> Query:
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
                'MATCH (e:Record) WHERE e.$attribute IS NOT NULL AND e.loadStatus = $load_status 
                AND NOT apoc.meta.cypher.isType(e.$attribute, "$date_type")
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
                         "date_type": datetime_object.get_date_type(),
                         "batch_size": batch_size,
                         "attribute": attribute,
                         "offset": offset,
                         "load_status": load_status
                     })

    @staticmethod
    def get_convert_epoch_to_timestamp_query(attribute, datetime_object, batch_size, load_status) -> Query:
        # language=SQL
        query_str = '''
                CALL apoc.periodic.iterate(
                'MATCH (e:Record) WHERE e.$attribute IS NOT NULL AND e.loadStatus = $load_status 
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
                         "label": "Record",
                         "batch_size": batch_size,
                         "unit": datetime_object.unit,
                         "datetime_object_format": datetime_object.format,
                         "load_status": load_status
                     })

    @staticmethod
    def get_finalize_import_events_query(batch_size) -> Query:
        # language=SQL
        query_str = '''
            CALL apoc.periodic.iterate(
                'MATCH (e:Record) 
                WHERE e.loadStatus IS NOT NULL 
                RETURN e',
                'REMOVE e.loadStatus',
                {batchSize:$batch_size, parallel:false})
            '''

        return Query(query_str=query_str,
                     parameters={"batch_size": batch_size})

    @staticmethod
    def get_filter_events_by_property_query(prop: str, load_status: int, values: Optional[List[str]] = None,
                                            exclude=True) -> Query:
        if values is None:  # match all events that have a specific property
            negation = "NOT" if exclude else ""
            # query to delete all events and its relationship with property
            # language=SQL
            query_str = '''
                    MATCH (e:Event {loadStatus: $load_status})
                    WHERE e.$prop IS $negation NULL
                    DETACH DELETE e
                    '''
            template_string_parameters = {"prop": prop, "negation": negation}
        else:  # match all events with specific property and value
            negation = "" if exclude else "NOT"
            # match all e and delete them and its relationship
            # language=SQL
            query_str = '''
                    MATCH (e:Event {loadStatus: $load_status})
                    WHERE $negation e.$prop IN $values
                    DETACH DELETE e
                '''
            template_string_parameters = {
                "prop": prop, "negation": negation, "values": values, "load_status": load_status
            }

        # execute query
        return Query(query_str=query_str,
                     template_string_parameters=template_string_parameters)

    @staticmethod
    def get_update_load_status_query(current_load_status: int, batch_size: int):
        query_str = '''
        CALL apoc.periodic.commit(
            'MATCH (record:Record) 
            WHERE record.loadStatus = $old_value
            WITH record LIMIT $limit
            SET record.loadStatus = record.loadStatus + 1
            RETURN COUNT(*)',
            {old_value: $old_value,
            limit: $limit})
                            '''
        return Query(query_str=query_str,
                     parameters={
                         "old_value": current_load_status,
                         "limit": batch_size
                     })
