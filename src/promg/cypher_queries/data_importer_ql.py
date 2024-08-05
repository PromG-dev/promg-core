from typing import Dict, Optional, List, Union
from string import Template

from ..data_managers.datastructures import DataStructure, DatetimeObject
from ..data_managers.semantic_header import RecordConstructor
from ..database_managers.db_connection import Query


class DataImporterQueryLibrary:

    @staticmethod
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

    @staticmethod
    def get_record_types_mapping(is_match, labels):
        if is_match:
            if len(labels) == 0:
                return "MATCH (record:Record)"
            labels = [f'''MATCH (record:Record) - [:IS_OF_TYPE] -> (:RecordType {{type:"{label}"}})''' for label in
                      labels]
            record_types = "\n".join(labels)
        else:
            labels = [(f'''MERGE ({label}_record:RecordType {{type:"{label}"}}) \n'''
                       f'''MERGE (record) - [:IS_OF_TYPE] -> ({label}_record)''') for label in labels]
            record_types = "\n".join(labels)
        return record_types

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
    def get_create_nodes_by_loading_csv_query(labels: List[str], file_name: str, mapping: str) -> Query:
        """
        Create event nodes for each row in the batch with labels
        The properties of each row are also the property of the node
        @param mapping: The dtype mapping of the imported records as string
        @param file_name: the name of the file to be imported
        @param labels: The labels of the record nodes

        @return: Query object to create record nodes by loading csv
        """

        # $batch is a variable we can add in tx.run, this allows us to use string properties
        # (keys in our dictionary are string)
        # return is required when using call and yield
        # language=SQL

        query_str = '''
                    CALL apoc.periodic.iterate('
                        CALL apoc.load.csv("$file_name" $mapping_str) yield map as row return row',
                        'CREATE (record:Record)
                        SET record += row
                        $record_types'
                    , {batchSize:10000, parallel:true, retries: 1});                    
                '''

        return Query(query_str=query_str,
                     template_string_parameters={
                         "file_name": file_name,
                         "record_types": DataImporterQueryLibrary.get_record_types_mapping(is_match=False,
                                                                                           labels=labels),
                         "mapping_str": DataImporterQueryLibrary.create_mapping_str(mapping)
                     })

    @staticmethod
    def get_make_timestamp_date_query(required_labels: List[str], attribute: str, datetime_object: DatetimeObject,
                                      load_status: int) -> Query:
        """
        Create a query to convert the strings of the timestamp to the datetime as used in Neo4j
        Remove the str_timestamp property

        @param required_labels: the required labels of the just imported nodes
        @param attribute: the name of the attribute that should be converted
        @param datetime_object: the DatetimeObject describing how the attribute should be converted
        @param load_status: the current load status of the records that are being imported

        @return: Query object to convert the timestamps string into timestamp objects

        """
        offset = datetime_object.timezone_offset
        offset = f'{attribute}+"{offset}"' if offset != "" else attribute

        # language=SQL
        query_str = '''
                CALL apoc.periodic.iterate(
                '$match_record_types 
                USING INDEX record:Record(loadStatus)
                WHERE record.$attribute IS NOT NULL AND record.loadStatus = $load_status 
                AND NOT apoc.meta.cypher.isType(record.$attribute, "$date_type")
                WITH record, record.$offset as timezone_dt
                WITH record, datetime(apoc.date.convertFormat(timezone_dt, "$datetime_object_format", 
                    "$datetime_object_convert_to")) as converted
                RETURN record, converted',
                'SET record.$attribute = converted',
                {batchSize:$batch_size, parallel:true})
            '''

        return Query(query_str=query_str,
                     template_string_parameters={
                         "match_record_types": DataImporterQueryLibrary.get_record_types_mapping(is_match=True,
                                                                                                 labels=required_labels),
                         "datetime_object_format": datetime_object.format,
                         "datetime_object_convert_to": datetime_object.convert_to,
                         "date_type": datetime_object.get_date_type(),
                         "attribute": attribute,
                         "offset": offset,
                         "load_status": load_status
                     })

    @staticmethod
    def get_convert_epoch_to_timestamp_query(required_labels: List[str], attribute: str,
                                             datetime_object: DatetimeObject,
                                             load_status: int) -> Query:
        """
        Create a query to convert epoch timestamp to the datetime as used in Neo4j
        Remove the str_timestamp property

        @param required_labels: the required labels of the just imported nodes
        @param attribute: the name of the attribute that should be converted
        @param datetime_object: the DatetimeObject describing how the attribute should be converted
        @param load_status: the current load status of the records that are being imported

        @return: Query object to convert the epoch timestamps into timestamp objects

        """

        # language=SQL
        query_str = '''
                CALL apoc.periodic.iterate(
                '$match_record_types 
                USING INDEX record:Record(loadStatus)
                WHERE record.$attribute IS NOT NULL AND record.loadStatus = $load_status 
                WITH record, record.$attribute as timezone_dt
                WITH record, apoc.date.format(timezone_dt, $unit, 
                    $dt_format) as converted
                RETURN record, converted',
                'SET record.$attribute = converted',
                {batchSize:$batch_size, parallel:false, 
                params: {unit: $unit,
                        dt_format: $datetime_object_format}})
            '''

        return Query(query_str=query_str,
                     template_string_parameters={
                         "attribute": attribute,
                         "match_record_types": DataImporterQueryLibrary.get_record_types_mapping(is_match=True,
                                                                                                 labels=required_labels),
                     },
                     parameters={
                         "unit": datetime_object.unit,
                         "datetime_object_format": datetime_object.format,
                         "load_status": load_status
                     })

    @staticmethod
    def get_finalize_import_records_query(required_labels: List[str], load_status: int) -> Query:
        """
        Create a query to finalize the import of the record nodes, i.e. remove the load status

        @param required_labels: the required labels of the just imported nodes
        @param load_status: the current load status of the records that are being imported

        @return: Query object to remove the load status attribute of the just imported nodes

        """

        # language=SQL
        query_str = '''
            CALL apoc.periodic.iterate(
                // find all correct record types with the correct load status
                '$match_record_types
                USING INDEX record:Record(loadStatus)
                WHERE record.loadStatus = $load_status
                RETURN record',
                // remove the status
                'REMOVE record.loadStatus',
                {batchSize:$batch_size, params:{load_status:$load_status}})
            '''

        return Query(query_str=query_str,
                     template_string_parameters={
                         "match_record_types": DataImporterQueryLibrary.get_record_types_mapping(is_match=True,
                                                                                                 labels=required_labels)
                     },
                     parameters={
                         "load_status": load_status
                     }
                     )

    @staticmethod
    def get_filter_records_by_property_query(prop: str, load_status: int, values: Optional[List[str]] = None,
                                             exclude: bool = True, required_labels=["Record"]) -> Query:
        """
        Create a query to remove nodes and their relationships if they have (exlude) or have not (include) a certain
        attribute or a certain attribute-value pairs.

        @param prop: the name of the property
        @param load_status: the current load status of the records that are being imported
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
                    // match all records that match loadstatus and property
                    '$match_record_types 
                    WHERE record.loadStatus = $load_status
                    WHERE record.$prop IS $negation NULL
                    RETURN record',
                    // delete record and its relationships
                    'DETACH DELETE record',
                    // pass the query parameters
                    {batchSize:$batch_size, params:{load_status:$load_status}})
                    '''
            template_string_parameters = {"prop": prop, "negation": negation}
        else:  # match all events with specific property and value
            negation = "" if exclude else "NOT"
            # match all r and delete them and its relationship
            # language=SQL
            query_str = '''
            CALL apoc.periodic.iterate(
                // match all records that match loadstatus and property
                    '$match_record_types 
                    WHERE record.loadStatus = $load_status
                    AND $negation record.$prop IN $values
                    RETURN record',
                    // delete record and its relationships
                    'DETACH DELETE record',
                    // pass the query parameters
                    {batchSize:$batch_size, params:{load_status:$load_status, values:$values}})
                    
                '''
            template_string_parameters = {
                "prop": prop,
                "negation": negation,
                "match_record_types": DataImporterQueryLibrary.get_record_types_mapping(is_match=True,
                                                                                        labels=required_labels)
            }

        # execute query
        return Query(query_str=query_str,
                     template_string_parameters=template_string_parameters,
                     parameters={
                         "load_status": load_status,
                         "values": values
                     })

    @staticmethod
    def get_update_load_status_query(current_load_status: int):
        """
        Create a query to update the record nodes that match the current load status with 1

        @param current_load_status: the current load status of the records that are being imported

        @return: Query object that updates the Record nodes that match the current load status with 1

        """

        # language=SQL
        query_str = '''
        CALL apoc.periodic.iterate(
        // match records with correct load status
            'MATCH (record:Record) 
            WHERE record.loadStatus = $old_value
            RETURN record',
            // update the load status
            'SET record.loadStatus = record.loadStatus + 1',
            {batchSize:$batch_size, params:{old_value:$old_value}})
                            '''
        return Query(query_str=query_str,
                     parameters={
                         "old_value": current_load_status
                     })
