from typing import Dict, Optional, List, Union
from string import Template
import re

from ..data_managers.datastructures import DataStructure, TemporalDefinition
from ..data_managers.semantic_header import RecordConstructor
from ..database_managers.db_connection import Query


# region Validation helpers

def _sanitize_identifier(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError(f"Invalid identifier: {value}")
    return value

# endregion

#region Cypher builders

def _build_match_records_with_types_clause(labels):
    if len(labels) == 0:
        return "MATCH (record:Record)"
    record_types = "\n".join(
        [f'''MATCH (record:Record) - [:IS_OF_TYPE] -> (:RecordType {{type:"{label}"}})''' for label in
         labels])
    return record_types


def _build_match_record_types_clause(labels: List[str]) -> str:
    return "\n".join(
        [f'MATCH ({_sanitize_identifier(label)}_record:RecordType {{type: "{label}"}})' for label in labels])


def _build_set_attributes_clause(cypher_conversion_mapping: Dict[str, str]):
    mapping = []
    for column_name, cypher_function in cypher_conversion_mapping.items():
        column = _sanitize_identifier(column_name)
        mapping.append(f'record.{column} = {cypher_function}(row.{column})')

    if not mapping:  # mapping is empty:
        return ""

    mapping_str = ", ".join(mapping)
    return f"SET {mapping_str}"


def _build_match_log_clause(log_name: Optional[str]):
    if log_name is None:
        return ''
    return f'''\n MATCH (log:Log {{name: $log_name}})'''


def _build_create_log_contains_relation_clause(log_name: Optional[str]):
    if log_name is None:
        return ''
    return '''CREATE (record)<-[:CONTAINS]-(log)'''


def _build_create_record_is_of_type_relation_clause(labels):
    return "\n".join(
        [f'''CREATE (record)-[:IS_OF_TYPE]->({_sanitize_identifier(label)}_record)''' for label in labels])


# endregion

class DataImporterQueryLibrary:
    @staticmethod
    def get_import_directory_query() -> Query:
        """
        Query that gets the import directory of the current running database

        :return: Query object to get import directory of the current running database
        """

        # language=cypher
        query_str = """
                    Call dbms.listConfig() YIELD name, value
                    WHERE name = 'server.directories.import'
                    RETURN value as directory \
                    """

        return Query(query_str=query_str)

    @staticmethod
    def get_create_record_types_and_log_query(labels: List[str], log_name: str = None) -> Query:
        query_str = "\n".join(
            [f'''MERGE (:RecordType {{type:"{label}"}})''' for label in labels])
        query_str += f'''\nMERGE (:Log {{name:"{log_name}"}})''' if log_name is not None else ""

        return Query(query_str=query_str)

    @staticmethod
    def build_create_nodes_by_loading_csv_query(labels: List[str],
                                                file_name: str,
                                                cypher_conversion_mapping: Dict[str, str],
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

        # language=cypher
        query_str = '''
                    :auto
                    LOAD CSV WITH HEADERS FROM '$file_name' AS row
                    CALL (row) {
                          $match_log_node
                          $match_record_type_clauses
                          CREATE (record:Record)
                          $create_log_relation
                          $create_type_relations
                          $set_records
                    } IN CONCURRENT TRANSACTIONS ON ERROR RETRY \
                    '''

        return Query(query_str=query_str,
                     template_string_parameters={
                         "file_name": f'file:///{file_name}',
                         "match_log_node": _build_match_log_clause(log_name=log_name),
                         "match_record_type_clauses": _build_match_record_types_clause(labels=labels),
                         "create_log_relation": _build_create_log_contains_relation_clause(log_name=log_name),
                         "create_type_relations": _build_create_record_is_of_type_relation_clause(labels=labels),
                         "set_records": _build_set_attributes_clause(cypher_conversion_mapping),
                     },
                     parameters={
                         "log_name": log_name,
                     })

    @staticmethod
    def get_filter_records_by_property_query(prop: str, values: Optional[List[str]] = None,
                                             exclude: bool = True, required_labels: Optional[List[str]] = None) -> Query:
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

        required_labels = required_labels or ["Record"]

        if values is None:  # match all events that have a specific property
            negation = "NOT" if exclude else ""
            # query to delete all records and its relationship with property
            # language=cypher
            query_str = '''
                        :auto
                        // match all records that match property
                        $match_record_types
                        WHERE record.$prop IS $negation NULL
                        WITH record
                        CALL (record) {
                            DETACH
                            DELETE record
                        } IN TRANSACTIONS ON ERROR RETRY \
                        '''
            template_string_parameters = {"prop": prop, "negation": negation}
        else:  # match all events with specific property and value
            negation = "" if exclude else "NOT"
            # match all r and delete them and its relationship
            # language=cypher
            query_str = '''
                        :auto
                        // match all records that match property
                        $match_record_types
                        WHERE $negation record.$prop IN $values
                        WITH record
                        CALL (record) {
                            // delete record and its relationships
                            DETACH
                            DELETE record
                        } IN TRANSACTIONS ON ERROR RETRY \
                        '''
            template_string_parameters = {
                "prop": prop,
                "negation": negation,
                "match_record_types": _build_match_records_with_types_clause(labels=required_labels)
            }

        # execute query
        return Query(query_str=query_str,
                     template_string_parameters=template_string_parameters,
                     parameters={
                         "values": values
                     })
