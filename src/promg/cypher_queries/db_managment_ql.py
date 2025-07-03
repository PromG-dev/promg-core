from typing import List

from ..data_managers.semantic_header import ConstructedNodes
from ..database_managers.db_connection import Query


class DBManagementQueryLibrary:
    @staticmethod
    def get_all_rel_types_query() -> Query:
        # find all relations and return the distinct types

        # language=SQL
        query_str = '''
                MATCH () - [rel] - () RETURN DISTINCT type(rel) AS rel_type
            '''

        return Query(query_str=query_str)

    @staticmethod
    def get_all_node_labels_query() -> Query:
        # find all nodes and return the distinct labels

        # language=SQL
        query_str = '''
            MATCH (n) RETURN DISTINCT labels(n) AS label
        '''

        return Query(query_str=query_str)

    @staticmethod
    def get_clear_db_query(db_name) -> Query:
        # language=SQL
        query_str = '''
            CREATE OR REPLACE DATABASE $db_name
            WAIT
        '''

        return Query(query_str=query_str, database="system", template_string_parameters={"db_name": db_name})

    @staticmethod
    def get_delete_relationships_query() -> Query:
        # language=SQL
        # add :auto as flag to indicate the query should run in an implicit transaction
        query_str = ''':auto 
                    MATCH ()-[r]->() 
                    CALL { WITH r 
                    DELETE r 
                    } IN TRANSACTIONS OF 50000 ROWS;
                '''

        return Query(query_str=query_str)

    @staticmethod
    def get_delete_nodes_query() -> Query:
        # language=SQL

        # add :auto as flag to indicate the query should run in an implicit transaction
        query_str = ''':auto
                MATCH (n)
                CALL { WITH n
                DELETE n
                } IN TRANSACTIONS OF 50000 ROWS;
            '''

        return Query(query_str=query_str)

    @staticmethod
    def get_replace_db_query(db_name) -> Query:
        # language=SQL
        query_str = '''
                    CREATE OR REPLACE DATABASE $db_name
                    WAIT
                '''

        return Query(query_str=query_str,
                     database="system",
                     template_string_parameters={"db_name": db_name})

    @staticmethod
    def get_constraints_query() -> Query:
        query_str = '''
            SHOW INDEX
        '''

        return Query(query_str=query_str)

    @staticmethod
    def get_constraint_unique_entity_uid_query(node_type=None, entity_key_name="sysId") -> Query:
        if node_type is None:
            node_type = "Entity"
        # language=SQL
        query_str = '''
            CREATE CONSTRAINT $constraint_name IF NOT EXISTS 
            FOR (en:$node_type) REQUIRE en.$entity_key_name IS UNIQUE
            // also set the range index
            OPTIONS {
              indexProvider: 'range-1.0'
            }
        '''
        return Query(query_str=query_str,
                     template_string_parameters={
                         "node_type": node_type,
                         "constraint_name": f"unique_{node_type.lower()}_ids",
                         "entity_key_name": entity_key_name
                     })

    @staticmethod
    def get_set_identifier_index_query(node_type: str, identifier_properties: List[str]):
        properties = f"( {','.join(['en.' + prop for prop in identifier_properties])})"
        # language=SQL
        query_str = '''
                    CREATE CONSTRAINT $constraint_name IF NOT EXISTS 
                    FOR (en:$node_type) REQUIRE $properties IS UNIQUE
                '''
        return Query(query_str=query_str,
                     template_string_parameters={
                         "node_type": node_type,
                         "constraint_name": f"unique_{node_type.lower()}_ids",
                         "properties": properties
                     })

    @staticmethod
    def get_set_unique_log_name_index_query() -> Query:
        # language=SQL
        query_str = '''
            CREATE CONSTRAINT unique_entity_ids IF NOT EXISTS 
            FOR (l:Log) REQUIRE l.name IS UNIQUE
        '''
        return Query(query_str=query_str)

    @staticmethod
    def get_set_sysid_index_query(entity_key_name) -> Query:
        # language=SQL
        query_str = '''
            CREATE RANGE INDEX entity_sys_id_index 
            IF NOT EXISTS FOR (n:Entity) ON (n.$entity_key_name)
        '''
        return Query(query_str=query_str,
                     template_string_parameters={
                         "entity_key_name": entity_key_name
                     })

    @staticmethod
    def get_set_activity_index_query() -> Query:
        # language=SQL
        query_str = '''
                CREATE RANGE INDEX activity_index 
                IF NOT EXISTS FOR (a:Activity) ON (a.activity)
            '''
        return Query(query_str=query_str)

    @staticmethod
    def get_set_record_id_as_range_query() -> Query:
        # language=SQL
        query_str = '''
                CREATE RANGE INDEX record_id_range 
                IF NOT EXISTS FOR (r:Record) ON (r.recordId)
        '''
        return Query(query_str=query_str)

    @staticmethod
    def get_set_record_type_range_query() -> Query:
        # language=SQL
        query_str = '''
                CREATE RANGE INDEX record_type_range 
                IF NOT EXISTS FOR (rt:RecordType) ON (rt.type)
        '''
        return Query(query_str=query_str)

    @staticmethod
    def get_node_count_query() -> Query:
        # language=SQL
        query_str = '''
                // List all node types and counts
                MATCH (n) 
                WITH n, CASE labels(n)[0]
                    WHEN 'Event' THEN 0
                    WHEN 'Entity' THEN 1
                    WHEN 'Class' THEN 2
                    WHEN 'Log' THEN 3
                    ELSE 4
                END AS sortOrder
                WITH  labels(n)[0] AS label,  count(n) AS numberOfNodes,sortOrder
                RETURN label,  numberOfNodes ORDER BY sortOrder
            '''

        return Query(query_str=query_str)

    @staticmethod
    def get_edge_count_query() -> Query:
        # language=SQL
        query_str = '''
                // List all agg rel types and counts
                MATCH () - [r] -> ()
                WHERE r.type is NOT NULL
                WITH r, CASE toUpper(r.type)
                  WHEN 'REL' THEN 0
                  WHEN 'DF' THEN 1
                  ELSE 2
                END as sortOrder
                WITH toUpper(r.type) as aggType, count(r) as aggNumberOfRelations, sortOrder
                RETURN aggType, aggNumberOfRelations ORDER BY sortOrder
            '''

        return Query(query_str=query_str)

    @staticmethod
    def get_aggregated_edge_count_query() -> Query:
        # language=SQL
        query_str = '''
                // List all rel types and counts
                MATCH () - [r] -> ()
                // WHERE r.type is  NULL
                WITH r, CASE Type(r)
                  WHEN 'CORR' THEN 0
                  WHEN 'OBSERVED' THEN 1
                  WHEN 'HAS' THEN 2
                  ELSE 3
                END as sortOrder
                WITH Type(r) as type, count(r) as numberOfRelations, sortOrder
                RETURN type, numberOfRelations ORDER BY sortOrder
            '''

        return Query(query_str=query_str)

    @staticmethod
    def get_imported_logs_query() -> Query:
        # language = SQL
        query_str = '''
            MATCH (l:Log)
            RETURN COLLECT(DISTINCT l.name) AS logs
        '''

        return Query(query_str=query_str)
