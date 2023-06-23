from ..data_managers.semantic_header import ConstructedNodes, ConstructedRelation, NodeConstructor, Relationship, Node, \
    RelationConstructor
from ..database_managers.db_connection import Query


class SemanticHeaderQueryLibrary:
    @staticmethod
    def get_create_node_by_record_constructor_query(node_constructor: NodeConstructor) -> Query:
        # find events that contain the entity as property and not nan
        # save the value of the entity property as id and also whether it is a virtual entity
        # create a new entity node if it not exists yet with properties
        if "Event" in node_constructor.get_labels():
            # language=SQL
            query_str = '''
                            MATCH ($record) WHERE $conditions
                            CREATE ($result_node)
                            '''
        else:
            # language=SQL
            query_str = '''
                            MATCH ($record) WHERE $conditions
                            MERGE ($result_node)
                            '''

        if node_constructor.set_labels is not None:
            query_str += '''
            SET $set_labels
            '''

        if node_constructor.set_properties is not None:
            query_str += '''
            SET $set_result_properties
            '''

        if node_constructor.infer_prevalence_record:
            # language=SQL
            query_str += '''
            MERGE (record) <- [:PREVALENCE] - ($result_node_name)
                '''
        if node_constructor.infer_corr_from_event_record:
            # language=SQL
            query_str += '''
            WITH record, $result_node_name
                                MATCH (event:Event) - [:PREVALENCE] -> (record) <- [:PREVALENCE] - ($result_node_name)
                                MERGE (event) - [:CORR] -> ($result_node_name)'''
        elif node_constructor.infer_observed:
            # language=SQL
            query_str += '''
            WITH record, $result_node_name
                                MATCH (event:Event) - [:PREVALENCE] -> (record) <- [:PREVALENCE] - ($result_node_name)
                                MERGE (event) <- [:OBSERVED] - ($result_node_name)
                                '''

        return Query(query_str=query_str,
                     template_string_parameters={
                         "record": node_constructor.get_prevalent_record_pattern(node_name="record"),
                         "conditions": node_constructor.get_where_condition(node_name="record"),
                         "result_node": node_constructor.result.get_pattern(),
                         "result_node_name": node_constructor.result.get_name(),
                         "set_result_properties": node_constructor.get_set_result_properties_query(),
                         "set_labels": node_constructor.get_set_result_labels_query()
                     })

    @staticmethod
    def get_create_nodes_by_relations_query(node_constructor: NodeConstructor) -> Query:
        if node_constructor.infer_reified_relation:
            # language=sql
            query_str = '''
                                   MATCH ($from_node) - [r:$rel_type] -> ($to_node)
                                   MERGE ($from_node_name) <- [:REIFIED] - ($result_node) - [:REIFIED] -> (
                                   $to_node_name)
                               '''
        else:
            # language=sql
            query_str = '''
                                   MATCH ($from_node) - [r:$rel_type] -> ($to_node)
                                   MERGE ($result_node)
                               '''
        if node_constructor.infer_corr_from_reified_parents:
            # language=sql
            query_str += '''WITH $from_node_name, $to_node_name, $result_node_name
                                   MATCH ($from_node_name) <- [:CORR] - (e:Event)
                                   MATCH ($to_node_name) <- [:CORR] - (f:Event)
                                   MERGE ($result_node_name) <- [:CORR] - (e)
                                   MERGE ($result_node_name) <- [:CORR] - (f)
                   '''

        # TODO from node
        return Query(query_str=query_str,
                     template_string_parameters={
                         "from_node": node_constructor.relation.from_node.get_pattern(),
                         "to_node": node_constructor.relation.to_node.get_pattern(),
                         "from_node_name": node_constructor.relation.from_node.get_name(),
                         "to_node_name": node_constructor.relation.to_node.get_name(),
                         "rel_type": node_constructor.relation.relation_type,
                         "result_node": node_constructor.result.get_pattern(),
                         "result_node_name": node_constructor.result.get_name()
                     })

    @staticmethod
    def get_create_relation_by_relations_query(relation_constructor: RelationConstructor, batch_size: int) -> Query:
        # language=SQL
        query_str = '''
                CALL apoc.periodic.iterate(
                '$relation_queries                        
                RETURN distinct $from_node_name, $to_node_name',
                'MERGE ($from_node_name) -[$rel_pattern] -> ($to_node_name)',                        
                {batchSize: $batch_size})
            '''

        return Query(query_str=query_str,
                     template_string_parameters={
                         "relation_queries": relation_constructor.get_relations_query(),
                         "from_node_name": relation_constructor.from_node.get_name(),
                         "to_node_name": relation_constructor.to_node.get_name(),
                         "rel_pattern": relation_constructor.result.get_pattern(),
                         "batch_size": batch_size
                     })

    @staticmethod
    def get_create_relation_using_record_query(relation_constructor: RelationConstructor) -> Query:
        # find events that are related to different entities of which one event also has a reference to the other entity
        # create a relation between these two entities

        query_str = '''
                            MATCH ($from_node) - [:PREVALENCE] -> (record)
                            MATCH ($to_node) - [:PREVALENCE] -> (record)
                            MATCH ($record_node)
                            MERGE ($from_node_name) -[$rel_pattern] -> ($to_node_name)
                        '''

        return Query(query_str=query_str,
                     template_string_parameters={
                         "from_node": relation_constructor.from_node.get_pattern(),
                         "from_node_name": relation_constructor.from_node.get_name(),
                         "to_node": relation_constructor.to_node.get_pattern(),
                         "to_node_name": relation_constructor.to_node.get_name(),
                         "record_node": relation_constructor.prevalent_record.get_pattern(name="record"),
                         "rel_pattern": relation_constructor.result.get_pattern()
                     })


    @staticmethod
    def get_create_directly_follows_query(entity: ConstructedNodes, batch_size) -> Query:
        # find the specific entities and events with a certain label correlated to that entity
        # order all events by time, order_nr and id grouped by a node n
        # collect the sorted nodes as a list
        # unwind the list from 0 to the one-to-last node
        # find neighbouring nodes and add an edge between

        # language=sql
        query_str = '''
             CALL apoc.periodic.iterate(
                'MATCH (n:$entity_labels_string) <-[:CORR]- (e)
                WITH n , e as nodes ORDER BY e.timestamp, ID(e)
                WITH n , collect (nodes) as nodeList
                UNWIND range(0,size(nodeList)-2) AS i
                WITH n , nodeList[i] as first, nodeList[i+1] as second
                RETURN first, second',
                'MERGE (first) -[df:$df_entity {entityType: "$entity_type"}]->(second)
                 SET df.type = "DF"
                ',
                {batchSize: $batch_size})
            '''

        return Query(query_str=query_str,
                     template_string_parameters={
                         "entity_labels_string": entity.get_label_string(),
                         "df_entity": entity.get_df_label(),
                         "entity_type": entity.node_type,
                         "batch_size": batch_size
                     })

    @staticmethod
    def get_merge_duplicate_df_entity_query(node: ConstructedNodes) -> Query:

        # language=sql
        query_str = '''
                        MATCH (n1:Event)-[r:$df_entity {entityType: '$entity_type'}]->(n2:Event)
                        WITH n1, n2, collect(r) AS rels
                        WHERE size(rels) > 1
                        // only include this and the next line if you want to remove the existing relationships
                        UNWIND rels AS r 
                        DELETE r
                        MERGE (n1)
                            -[:$df_entity {entityType: '$entity_type', count:size(rels), type: 'DF'}]->
                              (n2)
                    '''
        return Query(query_str=query_str,
                     template_string_parameters={
                         "entity_type": node.node_type,
                         "df_entity": node.get_df_label()
                     })

    @staticmethod
    def delete_parallel_directly_follows_derived(_type: str, node: Node):
        # language=sql
        query_str = '''
            MATCH (e1:Event) -[df:DF {entityType: "$type"}]-> (e2:Event)
            WHERE (e1:Event) -[:DF {entityType: "$original_entity_type"}]-> (e2:Event)
            DELETE df'''

        return Query(query_str=query_str,
                     template_string_parameters={
                         "type": _type,
                         "original_entity_type": node.get_label_str()
                     },
                     parameters={})

