from typing import Dict, Optional, List, Union
from string import Template

from ..data_managers.datastructures import DataStructure, DatetimeObject
from ..data_managers.semantic_header import RecordConstructor
from ..database_managers.db_connection import Query


def create_mapping_str(mapping: str) -> str:
    """
    Create the string including the information of the datatypes mapping used when importing records.

    :param mapping: The dtype mapping of the imported records as string
    :return: str containing the mapping in Cypher format
    """
    if mapping == "":
        return ""
    mapping_str = ''',{nullValues: [""], mapping:$mapping}''' if mapping != "" else ""
    mapping_str = Template(mapping_str).safe_substitute({"mapping": mapping})
    return mapping_str


def get_match_record_types_mapping(labels):
    if len(labels) == 0:
        return "MATCH (record:Record)"
    record_types = "\n".join(
        [f'''MATCH (record:Record) - [:IS_OF_TYPE] -> (:RecordType {{type:"{label}"}})''' for label in
         labels])
    return record_types


class DataImporterQueryLibrary:
    @staticmethod
    def get_import_directory_query() -> Query:
        """
        Query that gets the import directory of the current running database

        :return: Query object to get import directory of the current running database
        """

        # language=SQL
        query_str = """
            Call dbms.listConfig() YIELD name, value
            WHERE name='server.directories.import'
            RETURN value as directory
        """

        return Query(query_str=query_str)

    @staticmethod
    def get_create_record_types_and_log_query(labels: List[str], log_name: str = None) -> Query:
        query_str = "\n".join(
            [f'''MERGE (:RecordType {{type:"{label}"}})''' for label in labels])
        query_str += f'''\nMERGE (:Log {{name:"{log_name}"}})''' if log_name is not None else ""

        return Query(query_str=query_str)

    @staticmethod
    def get_create_nodes_by_loading_csv_query(labels: List[str], file_name: str, mapping: str,
                                              log_name: str = None) -> Query:
        """
        Create event nodes for each row in the batch with labels
        The properties of each row are also the property of the node
        @param mapping: The dtype mapping of the imported records as string
        @param file_name: the name of the file to be imported
        @param labels: The labels of the record nodes
        @param log_name: the name of the log to be imported

        @return: Query object to create record nodes by loading csv
        """

        if log_name is not None:
            match_log_str = '''\n MATCH (log:Log {name:$log_name})'''
            create_log_str = '''CREATE (record)<-[:CONTAINS]-(log)'''
        else:
            match_log_str = ""
            create_log_str = ""
            log_name = ""

        match_record_types = "\n".join(
            [f'''MATCH ({label}_record:RecordType {{type:"{label}"}})''' for label in labels])
        match_record_types += match_log_str

        create_records = "\n".join([f'''CREATE (record) - [:IS_OF_TYPE] -> ({label}_record)''' for label in labels])
        create_records += create_log_str

        # language=SQL
        query_str = '''
                    CALL apoc.periodic.iterate('
                        CALL apoc.load.csv("$file_name" $mapping_str) yield map as row return row',
                        '$match_record_types
                        CREATE (record:Record)
                        $create_records
                        SET record += row '
                    , {batchSize:$batch_size, parallel:true, retries: 1, params:{log_name: $log_name}});              
                          
                '''

        return Query(query_str=query_str,
                     template_string_parameters={
                         "file_name": file_name,
                         "mapping_str": create_mapping_str(mapping),
                         "match_record_types": match_record_types,
                         "create_records": create_records
                     },
                     parameters={
                         "log_name": log_name
                     })

    @staticmethod
    def get_make_timestamp_date_query(required_labels: List[str], attribute: str,
                                      datetime_object: DatetimeObject) -> Query:
        """
        Create a query to convert the strings of the timestamp to the datetime as used in Neo4j
        Remove the str_timestamp property

        @param required_labels: the required labels of the just imported nodes
        @param attribute: the name of the attribute that should be converted
        @param datetime_object: the DatetimeObject describing how the attribute should be converted

        @return: Query object to convert the timestamps string into timestamp objects

        """
        # language=SQL
        query_str = '''
                CALL apoc.periodic.iterate(
                '$match_record_types 
                WHERE record.$attribute IS NOT NULL
                RETURN record',
                'WITH record, toString(record.$attribute) AS ts, $dt_from as dt_from, $dt_to as dt_to, $offset as offset
                WITH record, dt_from, dt_to, ts, case offset
                        WHEN not null then ts + offset
                        ELSE ts 
                        END as new_ts
                CALL apoc.case([
                        // CASE 1: ISO string or datetime with offset already present -> use as is
                        ts CONTAINS "T" AND ts =~ ".*([+-][0-9]{2}:?[0-9]{2}|Z)$", 
                        "WITH record, datetime(ts) AS converted RETURN converted",
                        
                        // CASE 2: ISO string, but no offset -> append and convert
                        ts CONTAINS "T", 
                        "WITH record, datetime(new_ts) AS converted RETURN converted",
                        
                        // CASE 3: Epoch in seconds
                        ts =~ "^[0-9]{10}$", 
                        "WITH record, datetime({ epochSeconds: toInteger(ts) }) AS converted RETURN converted",

                        // CASE 4: Epoch in milliseconds
                        ts =~ "^[0-9]{13}$", 
                        "WITH record, datetime({ epochMillis: toInteger(ts) }) AS converted RETURN converted"
                    ],
                    // ELSE custom string -> parse using format
                    "
                    WITH record, datetime(apoc.date.convertFormat(new_ts, $dt_from, $dt_to)) AS converted 
                    RETURN converted",
                    {record: record, ts: ts, new_ts: new_ts, dt_from: dt_from, dt_to: dt_to}
                ) YIELD value
                WITH record, value.converted AS converted, new_ts
                SET record.not_converted_timestamp = new_ts
                SET record.$attribute = converted
                RETURN null',
                
                {batchSize:$batch_size, 
                parallel:true, 
                params: {dt_from: $dt_from,
                         dt_to: $dt_to,
                         offset: $offset}})
            '''

        return Query(query_str=query_str,
                     template_string_parameters={
                         "match_record_types": get_match_record_types_mapping(labels=required_labels),
                         "attribute": attribute
                     },
                     parameters={
                         "dt_to": datetime_object.convert_to,
                         "dt_from": datetime_object.format,
                         "offset": datetime_object.timezone_offset
                     })

    @staticmethod
    def get_convert_epoch_to_timestamp_query(required_labels: List[str], attribute: str,
                                             datetime_object: DatetimeObject) -> Query:
        """
        Create a query to convert epoch timestamp to the datetime as used in Neo4j
        Remove the str_timestamp property

        @param required_labels: the required labels of the just imported nodes
        @param attribute: the name of the attribute that should be converted
        @param datetime_object: the DatetimeObject describing how the attribute should be converted

        @return: Query object to convert the epoch timestamps into timestamp objects

        """

        # language=SQL
        # TODO update method to be in line with the one before
        query_str = '''
                CALL apoc.periodic.iterate(
                '$match_record_types 
                WHERE record.$attribute IS NOT NULL AND NOT apoc.meta.cypher.isType(record.$attribute, $date_type)
                WITH record, record.$attribute as timezone_dt
                WITH record, apoc.date.format(timezone_dt, $unit, $dt_format) as converted
                RETURN record, converted',
                'SET record.$attribute = converted',
                {batchSize:$batch_size, parallel:false, 
                params: {unit: $unit,
                        dt_format: $datetime_object_format,
                        date_type: $date_type}})
            '''

        return Query(query_str=query_str,
                     template_string_parameters={
                         "attribute": attribute,
                         "match_record_types": get_match_record_types_mapping(labels=required_labels)
                     },
                     parameters={
                         "unit": datetime_object.unit,
                         "datetime_object_format": datetime_object.format,
                         "date_type": datetime_object.convert_to.replace("ISO_", "")
                     })


@staticmethod
def get_filter_records_by_property_query(prop: str, values: Optional[List[str]] = None,
                                         exclude: bool = True, required_labels=["Record"]) -> Query:
    """
    Create a query to remove nodes and their relationships if they have (exlude) or have not (include) a certain
    attribute or a certain attribute-value pairs.

    @param prop: the name of the property
    @param values: a list of values that the property should (not) have for being removed
    @param exclude: boolean indicating whether nodes should be removed if they match the criteria (exclude=True)
    or be kept (exclude = False)
    @param required_labels: the labels the nodes should have

    @return: Query object to remove the load status attribute of the just imported nodes

    """

    if values is None:  # match all events that have a specific property
        negation = "NOT" if exclude else ""
        # query to delete all records and its relationship with property
        # language=SQL
        query_str = '''
                    CALL apoc.periodic.iterate(
                    // match all records that match property
                    '$match_record_types 
                    WHERE record.$prop IS $negation NULL
                    RETURN record',
                    // delete record and its relationships
                    'DETACH DELETE record',
                    // pass the query parameters
                    {batchSize:$batch_size})
                    '''
        template_string_parameters = {"prop": prop, "negation": negation}
    else:  # match all events with specific property and value
        negation = "" if exclude else "NOT"
        # match all r and delete them and its relationship
        # language=SQL
        query_str = '''
            CALL apoc.periodic.iterate(
                // match all records that match property
                    '$match_record_types 
                    WHERE $negation record.$prop IN $values
                    RETURN record',
                    // delete record and its relationships
                    'DETACH DELETE record',
                    // pass the query parameters
                    {batchSize:$batch_size, params:{values:$values}})
                    
                '''
        template_string_parameters = {
            "prop": prop,
            "negation": negation,
            "match_record_types": get_match_record_types_mapping(labels=required_labels)
        }

    # execute query
    return Query(query_str=query_str,
                 template_string_parameters=template_string_parameters,
                 parameters={
                     "values": values
                 })
