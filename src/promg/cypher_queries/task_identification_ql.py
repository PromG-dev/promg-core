from string import Template

from ..database_managers.db_connection import Query


class TaskIdentifierLibrary:
    @staticmethod
    def get_combine_df_joint_query(resource, case):
        # language=sql
        query_str = '''
                CALL apoc.periodic.iterate(
                "MATCH (e1:Event)-[:$df_resource]->(e2:Event)
                 WHERE (e1)-[:$df_case]->(e2)
                 RETURN e1,e2",
                "WITH e1,e2
                    MERGE (e1)-[:DF_JOINT]->(e2)",
                    {batchSize:$batch_size})
                '''

        return Query(query_str=query_str,
                     template_string_parameters={
                         "df_resource": resource.get_df_label(),
                         "df_case": case.get_df_label()
                     })

    @staticmethod
    def get_create_task_instances_query(resource):
        print("get_create_task_instances_query")
        # language=sql
        query_str = '''
                CALL apoc.periodic.iterate(
                "CALL {
                    MATCH (e1:Event)-[:DF_JOINT]->() WHERE NOT ()-[:DF_JOINT]->(e1)
                    MATCH ()-[:DF_JOINT]->(e2:Event) WHERE NOT (e2)-[:DF_JOINT]->()
                    MATCH p=(e1)-[:DF_JOINT*]->(e2)
                    RETURN p, e1, e2
                    UNION
                    MATCH (e:Event) WHERE (e)-[:CORR]->(:$resource_node_label)
                    AND NOT ()-[:DF_JOINT]->(e) AND NOT (e)-[:DF_JOINT]->()
                    MATCH p=(e) RETURN p, e AS e1, e AS e2
                 }
                 RETURN [event in nodes(p) | event.activity+'+'+event.lifecycle] AS variant,
                    nodes(p) AS events, e1.timestamp AS start_time, e2.timestamp AS end_time",
                "WITH variant, events, start_time, end_time
                    CREATE (ti:TaskInstance {variant:variant, start_time:start_time, end_time:end_time})
                    WITH ti, events
                    UNWIND events AS e
                        CREATE (e)<-[:CONTAINS]-(ti)",
                {batchSize:$batch_size})
                '''

        return Query(query_str=query_str,
                     template_string_parameters={
                         "resource_node_label": resource.type
                     })

    @staticmethod
    def get_split_ti_nodes_create_new_1_query():
        print("get_split_ti_nodes_create_new_1_query")
        # language=sql
        query_str = '''
                CALL apoc.periodic.iterate(
                "MATCH (ti:TaskInstance)-[:CONTAINS]->(e:Event) WHERE date(ti.start_time) <> date(ti.end_time)
                 WITH ti, date(e.timestamp) AS date, e ORDER BY e.timestamp
                 WITH DISTINCT ti, date, COLLECT(e) AS events
                 WITH events[0] AS e_start, events[size(events)-1] AS e_end
                 WITH e_start, e_end
                 MATCH p=(e_start)-[:DF_JOINT*]->(e_end)
                 WITH p, e_start AS e1, e_end AS e2
                 RETURN [event in nodes(p) | event.activity+'+'+event.lifecycle] AS variant, 
                    nodes(p) AS events, e1.timestamp AS start_time, e2.timestamp AS end_time",
                "WITH variant, events, start_time, end_time
                    CREATE (ti:TaskInstance {variant:variant, start_time:start_time, end_time:end_time})
                    WITH ti, events
                    UNWIND events AS e
                    CREATE (e)<-[:CONTAINS]-(ti)",
                {batchSize:$batch_size})
                '''

        return Query(query_str=query_str)

    @staticmethod
    def get_split_ti_nodes_create_new_2_query():
        print("get_split_ti_nodes_create_new_2_query")
        # language=sql
        query_str = '''
                CALL apoc.periodic.iterate(
                "MATCH (ti:TaskInstance)-[:CONTAINS]->(e:Event) WHERE date(ti.start_time) <> date(ti.end_time)
                 WITH ti, date(e.timestamp) AS date, e ORDER BY e.timestamp
                 WITH DISTINCT ti, date, COLLECT(e) AS events
                 WITH events[0] AS e_start, events[size(events)-1] AS e_end
                 WITH e_start, e_end
                 MATCH (e_start) MATCH (e_end) WHERE e_start = e_end
                 MATCH p=(e_start)
                 WITH p, e_start AS e1, e_end AS e2
                 RETURN [event in nodes(p) | event.activity+'+'+event.lifecycle] AS variant, 
                    nodes(p) AS events, e1.timestamp AS start_time, e2.timestamp AS end_time",
                "WITH variant, events, start_time, end_time
                 CREATE (ti:TaskInstance {variant:variant, start_time:start_time, end_time:end_time})
                 WITH ti, events
                 UNWIND events AS e
                 CREATE (e)<-[:CONTAINS]-(ti)",
                {batchSize:$batch_size})
                '''

        return Query(query_str=query_str)

    @staticmethod
    def get_split_ti_nodes_remove_old_query():
        print("get_split_ti_nodes_remove_old_query")
        # language=sql
        query_str = '''
                CALL apoc.periodic.iterate(
                "MATCH (ti:TaskInstance) WHERE date(ti.start_time) <> date(ti.end_time)
                 RETURN ti",
                "WITH ti
                 DETACH DELETE ti",
                {batchSize:$batch_size})
                '''

        return Query(query_str=query_str)

    @staticmethod
    def get_remove_df_joint_query():
        print("get_remove_df_joint_query")
        # language=sql
        query_str = '''
                MATCH ()-[r:DF_JOINT]-()
                DELETE r
                '''

        return Query(query_str=query_str)

    @staticmethod
    def get_correlate_ti_to_entity_query(entity):
        print("get_correlate_ti_to_entity_query")
        # language=sql
        query_str = '''
                CALL apoc.periodic.iterate(
                "MATCH (ti:TaskInstance)-[:CONTAINS]->(:Event)-[:CORR]->(n:$entity_node_label)
                 RETURN DISTINCT ti, n",
                "WITH ti, n
                    CREATE (ti)-[:CORR]->(n)",
                {batchSize:$batch_size})
                    '''

        return Query(query_str=query_str,
                     template_string_parameters={
                         "entity_node_label": entity.type
                     })

    @staticmethod
    def get_lift_df_to_task_instances_query(entity):
        print("get_lift_df_to_task_instances_query")
        # language=sql
        query_str = '''
                CALL apoc.periodic.iterate(
                "MATCH (n:$entity_node_label)
                 MATCH (ti:TaskInstance)-[:CORR]->(n)
                 WITH n, ti AS nodes ORDER BY ti.start_time, ID(ti)
                 WITH n, COLLECT (nodes) as nodeList
                 UNWIND range(0, size(nodeList)-2) AS i
                 RETURN n, nodeList[i] as ti_first, nodeList[i+1] as ti_second",
                "WITH n, ti_first, ti_second
                    MERGE (ti_first)-[df:DF_TI_$entity_node_label]->(ti_second)",
                {batchSize:$batch_size})
                    '''

        return Query(query_str=query_str,
                     template_string_parameters={
                         "entity_node_label": entity.type
                     })

    @staticmethod
    def get_aggregate_task_instances_query(property):
        print("get_aggregate_task_instances_query")
        # language=sql
        query_str = '''
                CALL apoc.periodic.iterate(
                "MATCH (ti:TaskInstance) WHERE ti.$property IS NOT NULL
                 RETURN DISTINCT ti.$property AS id, count(*) AS count",
                 "WITH id, count
                  MERGE (ta:TaskAggregation {Type:'$property', id:id, count:count})",
                {batchSize:$batch_size})
                '''

        return Query(query_str=query_str,
                     template_string_parameters={
                         "property": property
                     })

    @staticmethod
    def get_link_task_instances_to_aggregations_query(property):
        print("get_link_task_instances_to_aggregations_query")
        # language=sql
        query_str = '''
                CALL apoc.periodic.iterate(
                "MATCH (ta:TaskAggregation) WHERE ta.Type = '$property'
                 MATCH (ti:TaskInstance) WHERE ti.$property = ta.id
                 RETURN ta, ti",
                "WITH ta, ti
                 CREATE (ti)-[:OBSERVED]->(ta)",
                {batchSize:$batch_size})
                '''

        return Query(query_str=query_str,
                     template_string_parameters={
                         "property": property
                     })

    @staticmethod
    def get_lift_df_to_task_aggregations_query(property, entity):
        print("get_lift_df_to_task_aggregations_query")
        # language=sql
        query_str = '''
                MATCH (ta1:TaskAggregation)<-[:OBSERVED]-(ti1:TaskInstance)-[df:DF_TI_$entity_node_label]
                    ->(ti2:TaskInstance)-[:OBSERVED]->(ta2:TaskAggregation)
                    WHERE ta1.Type = '$property' AND ta2.Type = '$property'
                MATCH (ti1)-[:CORR]->(n:$entity_node_label)<-[:CORR]-(ti2)
                WITH ta1, count(df) AS df_freq, ta2
                MERGE (ta1)-[rel2:DF_TA_$entity_node_label]->(ta2) ON CREATE SET rel2.count=df_freq
                '''

        return Query(query_str=query_str,
                     template_string_parameters={
                         "property": property,
                         "entity_node_label": entity.type
                     })
