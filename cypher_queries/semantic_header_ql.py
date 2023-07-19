from string import Template

from ..data_managers.semantic_header import ConstructedNodes, ConstructedRelation, NodeConstructor, Relationship, \
    Node, \
    RelationConstructor
from ..database_managers.db_connection import Query


class SemanticHeaderQueryLibrary:
    @staticmethod
    def get_create_node_by_record_constructor_query(node_constructor: NodeConstructor, batch_size: int = 5000,
                                                    merge=False) -> Query:
        # find events that contain the entity as property and not nan
        # save the value of the entity property as id and also whether it is a virtual entity
        # create a new entity node if it not exists yet with properties
        merge_or_create = 'MERGE' if merge else 'CREATE'
        set_label_str = ""
        set_property_str = ""
        infer_corr_str = ""
        infer_observed_str = ""

        if node_constructor.set_labels is not None:
            set_label_str = f'SET $set_labels'''

        if node_constructor.set_properties is not None:
            set_property_str = 'SET $set_result_properties'

        if node_constructor.infer_corr_from_event_record:
            # language=SQL
            infer_corr_str = '''
            WITH record, $result_node_name
                                MATCH (event:Event) - [:PREVALENCE] -> (record) <- [:PREVALENCE] - ($result_node_name)
                                CREATE (event) - [:CORR] -> ($result_node_name)'''
        elif node_constructor.infer_corr_from_entity_record:  # TODO update such that only correct events are considered
            # language=SQL
            infer_corr_str = '''
                        WITH record, $result_node_name
                                MATCH (event:Event) - [:PREVALENCE] -> (record) <- [:PREVALENCE] - (
                                $result_node_name)
                                CREATE (event) - [:CORR] -> ($result_node_name)'''
        elif node_constructor.infer_observed:
            # language=SQL
            infer_observed_str = '''
            WITH record, $result_node_name
                                MATCH (event:Event) - [:PREVALENCE] -> (record) <- [:PREVALENCE] - ($result_node_name)
                                CREATE (event) <- [:OBSERVED] - ($result_node_name)
                                '''

        # language=SQL
        query_str = '''
                    CALL apoc.periodic.commit(
                        'MATCH ($record) 
                            WHERE record.created IS NULL
                            AND $conditions 
                            WITH record limit $limit
                            $merge_or_create ($result_node)
                            SET record.created = True
                            $set_label_str
                            $set_property_str
                            CREATE (record) <- [:PREVALENCE] - ($result_node_name)
                            $infer_corr_str
                            $infer_observed_str
                            RETURN count(*)',
                            {limit: $limit})
                    '''

        query_str = Template(query_str).safe_substitute({
            "set_label_str": set_label_str,
            "set_property_str": set_property_str,
            "infer_corr_str": infer_corr_str,
            "infer_observed_str": infer_observed_str,
            "merge_or_create": merge_or_create,
        })

        return Query(query_str=query_str,
                     template_string_parameters={
                         "record": node_constructor.get_prevalent_record_pattern(node_name="record"),
                         "record_name": "record",
                         "conditions": node_constructor.get_where_condition(node_name="record"),
                         "result_node": node_constructor.result.get_pattern(),
                         "result_node_name": node_constructor.result.get_name(),
                         "set_result_properties": node_constructor.get_set_result_properties_query(),
                         "set_labels": node_constructor.get_set_result_labels_query()
                     },
                     parameters={"limit": batch_size})

    @staticmethod
    def get_reset_created_record_query(node_constructor: NodeConstructor, batch_size: int):
        query_str = '''
                    CALL apoc.periodic.commit(
                        'MATCH ($record) 
                            WHERE record.created = True
                            WITH record limit $limit
                            SET record.created = Null
                            RETURN count(*)',
                            {limit: $limit})
                    '''
        return Query(query_str=query_str,
                     template_string_parameters={
                         "record": node_constructor.get_prevalent_record_pattern(node_name="record"),
                     },
                     parameters={"limit": batch_size * 10})

    @staticmethod
    def get_number_of_ids_query(node_constructor: NodeConstructor, use_record: bool = False):
        query_str = '''MATCH (n:$labels)
                        RETURN count(DISTINCT n.sysId) as num_ids'''

        if use_record:
            query_str = '''MATCH ($record) 
                           RETURN count(DISTINCT record.$attribute) as num_ids'''

        return Query(query_str=query_str,
                     template_string_parameters={
                         "labels": node_constructor.get_label_string(),
                         "attribute": node_constructor.result.properties[0].ref_attribute,
                         "record": node_constructor.get_prevalent_record_pattern(node_name="record")
                     })

    @staticmethod
    def get_merge_nodes_with_same_id_query(node_constructor: NodeConstructor, batch_size: int):
        if "Event" in node_constructor.get_labels():
            return None

        query_str = '''
                    CALL apoc.periodic.commit(
                           'MATCH (n:$labels)
                           WITH n LIMIT $limit
                           WITH $idt_properties, collect(n) as same_nodes
                           WHERE size(same_nodes) > 1
                           UNWIND range(0, toInteger(floor(size(same_nodes)/100))) as i
                           WITH same_nodes, i*100 as min_range, apoc.coll.min([(i+1)*100, size(same_nodes)]) AS 
                           max_range
                           WITH same_nodes[min_range..max_range] as lim_nodes
                           CALL apoc.refactor.mergeNodes(lim_nodes, {properties: "discard", mergeRels: true})
                           YIELD node
                           RETURN COUNT(*)',
                           {limit:$limit})
                       '''
        return Query(query_str=query_str,
                     template_string_parameters={
                         "labels": node_constructor.get_label_string(),
                         "idt_properties": node_constructor.get_idt_properties_query()
                     },
                     parameters={"limit": batch_size})

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
    def get_create_directly_follows_query(entity: ConstructedNodes, batch_size, event_label) -> Query:
        # find the specific entities and events with a certain label correlated to that entity
        # order all events by time, order_nr and id grouped by a node n
        # collect the sorted nodes as a list
        # unwind the list from 0 to the one-to-last node
        # find neighbouring nodes and add an edge between

        # language=sql

        if event_label == "CompoundEvent":
            if entity.node_type == "Resource":
                query_str = '''
                     CALL apoc.periodic.iterate(
                        'MATCH (n:$entity_labels_string) <-[:CORR]- (e:$event_label)
                        CALL {
                                WITH e
                                MATCH (e) - [:CONSISTS_OF] -> (single_event:Event)
                                RETURN id(single_event) as min_id ORDER BY id(single_event)
                                LIMIT 1
                            }
                        WITH n , e as nodes ORDER BY e.timestamp, min_id
                        WITH n , collect (nodes) as nodeList
                        UNWIND range(0,size(nodeList)-2) AS i
                        WITH n , nodeList[i] as first, nodeList[i+1] as second
                        RETURN n, first, second',
                        'MERGE (first) -[df:$df_entity {entityType: "$entity_type"}]->(second)
                         SET df.type = "DF"
                         SET df.entityId = n.sysId
                        ',
                        {batchSize: $batch_size})
                    '''
            else:
                query_str = '''
                                     CALL apoc.periodic.iterate(
                                        'MATCH (n:$entity_labels_string) <-[:CORR]- (e:$event_label)
                                        CALL {
                                                WITH e
                                                MATCH (e) - [:CONSISTS_OF] -> (single_event:Event)
                                                RETURN id(single_event) as min_id ORDER BY id(single_event)
                                                LIMIT 1
                                            }
                                        WITH n , e as nodes ORDER BY e.timestamp, min_id
                                        WITH n , collect (nodes) as nodeList
                                        UNWIND range(0,size(nodeList)-2) AS i
                                        WITH n , nodeList[i] as first, nodeList[i+1] as second
                                        RETURN first, second',
                                        'MERGE (first) -[df:$df_entity {entityType: "$entity_type"}]->(second)
                                         SET df.type = "DF"
                                        ',
                                        {batchSize: $batch_size})
                                    '''

        else:
            query_str = '''
                             CALL apoc.periodic.iterate(
                                'MATCH (n:$entity_labels_string) <-[:CORR]- (e:$event_label)
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
                         "event_label": event_label,
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
