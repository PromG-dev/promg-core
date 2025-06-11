from string import Template

from ..database_managers.db_connection import Query


class TaskIdentifierLibrary:
    @staticmethod
    def get_combine_df_joint_query(resource, case):
        # language=sql
        query_str = '''
                CALL apoc.periodic.iterate(
                "MATCH (e1:Event)-[:$df_resource]->(e2:Event)
                 WHERE (e1)-[:$df_case]->(e2) AND date(e1.timestamp) = date(e2.timestamp)
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
        # language=sql
        query_str = '''
                CALL apoc.periodic.iterate(
                "CALL {
                    MATCH (e1:Event)-[:DF_JOINT]->() WHERE NOT ()-[:DF_JOINT]->(e1)
                    MATCH ()-[:DF_JOINT]->(e2:Event) WHERE NOT (e2)-[:DF_JOINT]->()
                    MATCH p=(e1)-[:DF_JOINT*]->(e2)
                    RETURN p, e1, e2
                    UNION
                    MATCH (e:Event) WHERE (e)-[:$corr_type]->(:$resource_node_label)
                    AND NOT ()-[:DF_JOINT]->(e) AND NOT (e)-[:DF_JOINT]->()
                    MATCH p=(e) RETURN p, e AS e1, e AS e2
                 }
                     RETURN [event in nodes(p) | 
                        CASE 
                            WHEN event.lifecycle IS NOT NULL THEN event.activity + '+' + event.lifecycle 
                            ELSE event.activity 
                        END
                    ] AS variant,
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
                         "resource_node_label": resource.type,
                         "corr_type": resource.get_corr_type_strings()
                     })

    @staticmethod
    def get_remove_df_joint_query():
        # language=sql
        query_str = '''
                MATCH ()-[r:DF_JOINT]-()
                DELETE r
                '''

        return Query(query_str=query_str)

    @staticmethod
    def get_correlate_ti_to_entity_query(entity):
        # language=sql
        query_str = '''
                CALL apoc.periodic.iterate(
                "MATCH (ti:TaskInstance)-[:CONTAINS]->(:Event)-[:$corr_type]->(n:$entity_node_label)
                 RETURN DISTINCT ti, n",
                "WITH ti, n
                    CREATE (ti)-[:$corr_type]->(n)",
                {batchSize:$batch_size})
                    '''

        return Query(query_str=query_str,
                     template_string_parameters={
                         "entity_node_label": entity.type,
                         "corr_type": entity.get_corr_type_strings()
                     })

    @staticmethod
    def get_lift_df_to_task_instances_query(entity):
        # language=sql
        query_str = '''
                MATCH (ti1:TaskInstance)-[:CONTAINS]->(e1:Event)-[:$df_entity]
                    ->(e2:Event)<-[:CONTAINS]-(ti2:TaskInstance) WHERE ti1 <>ti2
                MERGE (ti1)-[df:$df_ti_entity {entityType:"$entity_node_label"}]->(ti2)
                    '''

        return Query(query_str=query_str,
                     template_string_parameters={
                         "df_entity": entity.get_df_label(),
                         "entity_node_label": entity.type,
                         "df_ti_entity": entity.get_df_ti_label()
                     })

    @staticmethod
    def get_aggregate_task_instances_query(property):
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
        # language=sql
        query_str = '''
                MATCH (ta1:TaskAggregation)<-[:OBSERVED]-(ti1:TaskInstance)-[df:DF_TI_$entity_node_label]
                    ->(ti2:TaskInstance)-[:OBSERVED]->(ta2:TaskAggregation)
                    WHERE ta1.Type = '$property' AND ta2.Type = '$property'
                MATCH (ti1)-[:CORR]->(n:$entity_node_label)<-[:CORR]-(ti2)
                WITH ta1, count(df) AS df_freq, ta2
                MERGE (ta1)-[rel2:$df_ta_entity {entityType: '$entity_node_label'}]->(ta2) ON CREATE SET rel2.count=df_freq
                '''

        return Query(query_str=query_str,
                     template_string_parameters={
                         "property": property,
                         "entity_node_label": entity.type,
                         "df_ta_entity": entity.get_df_ta_label()
                     })
