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

        return Query(query_str=query_str,
                     template_string_parameters={},
                     parameters={})

    @staticmethod
    def get_all_node_labels_query() -> Query:
        # find all nodes and return the distinct labels

        # language=SQL
        query_str = '''
            MATCH (n) RETURN DISTINCT labels(n) AS label
        '''

        return Query(query_str=query_str,
                     template_string_parameters={},
                     parameters={})

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
        query_str = '''
                    CALL apoc.periodic.iterate(
                        "MATCH () - [r] -> () return id(r) as id", 
                        "MATCH () - [r] -> () WHERE id(r) = id DELETE r", 
                        {batchSize:$batch_size})
                    yield batches, total 
                    RETURN batches, total
                '''

        return Query(query_str=query_str)

    @staticmethod
    def get_delete_nodes_query() -> Query:
        # language=SQL
        query_str = '''
                CALL apoc.periodic.iterate(
                    "MATCH (n) return id(n) as id", 
                    "MATCH (n) WHERE id(n) = id DETACH DELETE n", {batchSize:$batch_size})
                yield batches, total 
                RETURN batches, total
            '''

        return Query(query_str=query_str)

    @staticmethod
    def get_replace_db_query(db_name) -> Query:
        # language=SQL
        query_str = '''
                    CREATE OR REPLACE DATABASE $db_name
                    WAIT
                '''

        return Query(query_str=query_str, database="system", template_string_parameters={"db_name": db_name})

    @staticmethod
    def get_constraint_unique_event_id_query() -> Query:
        # language=SQL
        query_str = '''
            CREATE CONSTRAINT unique_event_ids IF NOT EXISTS 
            FOR (e:Event) REQUIRE e.ID IS UNIQUE
        '''
        return Query(query_str=query_str,
                     template_string_parameters={},
                     parameters={})

    @staticmethod
    def get_constraint_unique_entity_uid_query() -> Query:
        # language=SQL
        query_str = '''
            CREATE CONSTRAINT unique_entity_ids IF NOT EXISTS 
            FOR (en:Entity) REQUIRE en.sysId IS UNIQUE
        '''
        return Query(query_str=query_str,
                     template_string_parameters={},
                     parameters={})

    @staticmethod
    def get_constraint_unique_log_id_query() -> Query:
        # language=SQL
        query_str = '''
            CREATE CONSTRAINT unique_entity_ids IF NOT EXISTS 
            FOR (l:Log) REQUIRE l.ID IS UNIQUE
        '''
        return Query(query_str=query_str,
                     template_string_parameters={},
                     parameters={})

    @staticmethod
    def get_set_sysid_index_query() -> Query:
        # language=SQL
        query_str = '''
            CREATE INDEX entity_sys_id_index IF NOT EXISTS FOR (n:Entity) ON (n.sysId)
        '''
        return Query(query_str=query_str,
                     template_string_parameters={},
                     parameters={})

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
