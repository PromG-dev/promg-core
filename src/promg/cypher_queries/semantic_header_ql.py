from string import Template

from ..data_managers.semantic_header import ConstructedNodes, NodeConstructor, Node, \
    RelationConstructor, RecordConstructor
from ..database_managers.db_connection import Query


class SemanticHeaderQueryLibrary:
    @staticmethod
    def get_create_node_by_record_constructor_query(node_constructor: NodeConstructor, batch_size: int = 5000, merge=True) -> Query:
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

        if len(node_constructor.result.optional_properties) > 0:
            set_property_str = 'SET $set_result_properties'

        if len(node_constructor.inferred_relationships) > 0:
            infer_corr_str = '''WITH record, $result_node_name'''
            for relationship in node_constructor.inferred_relationships:
                infer_rel_str = '''
                    CALL {WITH record, $result_node_name
                            MATCH ($event_node) - [:PREVALENCE] -> (record:$record_labels) <- [:PREVALENCE] - (
                                $result_node_name)
                                MERGE (event) - [:$relation_type] -> ($result_node_name)}'''
                infer_rel_str = Template(infer_rel_str).safe_substitute({
                    "event_node": relationship.event.get_pattern(name="event"),
                    "record_labels": relationship.get_labels_str(),
                    "relation_type": relationship.relation_type
                })
                infer_corr_str += infer_rel_str
        elif node_constructor.infer_corr_from_event_record:
            # language=SQL
            infer_corr_str = '''
            WITH record, $result_node_name
                                MATCH (event:$event_label) - [:PREVALENCE] -> (record) <- [:PREVALENCE] - (
                                $result_node_name)
                                MERGE (event) - [:$corr_type] -> ($result_node_name)'''
        elif node_constructor.infer_corr_from_entity_record:  # TODO update such that only correct events are considered
            # language=SQL
            infer_corr_str = '''
                        WITH record, $result_node_name
                                MATCH (event:$event_label) - [:PREVALENCE] -> (record) <- [:PREVALENCE] - (
                                $result_node_name)
                                MERGE (event) - [:$corr_type] -> ($result_node_name)'''
        elif node_constructor.infer_observed:
            # language=SQL
            infer_observed_str = '''
            WITH record, $result_node_name
                                MATCH (event:$event_label) - [:PREVALENCE] -> (record) <- [:PREVALENCE] - (
                                $result_node_name)
                                CREATE (event) <- [:OBSERVED] - ($result_node_name)
                                '''

        # language=SQL
        query_str = '''
                    CALL apoc.periodic.commit(
                        'MATCH ($record) 
                        WHERE NOT record:RecordCreated
                            WITH $record_name limit $limit
                            $merge_or_create ($result_node)
                            SET $record_name:RecordCreated
                            $set_label_str
                            $set_property_str
                            MERGE (record) <- [:PREVALENCE] - ($result_node_name)
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
                         "conditions": node_constructor.get_where_condition(node_name="record", include_start_and=True),
                         "result_node": node_constructor.result.get_pattern(),
                         "result_node_name": node_constructor.result.get_name(),
                         "set_result_properties": node_constructor.get_set_result_properties_query(),
                         "set_labels": node_constructor.get_set_result_labels_query(),
                         "corr_type": node_constructor.corr_type,
                         "event_label": node_constructor.event_label
                     },
                     parameters={"limit": batch_size})

    @staticmethod
    def get_reset_created_record_query(batch_size: int):
        query_str = '''
                    CALL apoc.periodic.commit(
                        'MATCH (record:RecordCreated) 
                            WITH record limit $limit
                            REMOVE record:RecordCreated
                            RETURN count(*)',
                            {limit: $limit})
                    '''
        return Query(query_str=query_str,
                     parameters={"limit": batch_size})

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
                         "attribute": node_constructor.result.required_properties[0].ref_attribute,
                         "record": node_constructor.get_prevalent_record_pattern(node_name="record")
                     })

    @staticmethod
    def get_number_of_records_query(node_constructor: NodeConstructor):

        query_str = '''MATCH ($record) 
                           RETURN count(record) as count'''

        return Query(query_str=query_str,
                     template_string_parameters={
                         "record": node_constructor.get_prevalent_record_pattern(node_name="record")
                     })

    @staticmethod
    def get_reset_merged_in_nodes_query(node_constructor: NodeConstructor, batch_size: int):
        query_str = '''
                           CALL apoc.periodic.commit(
                                  'MATCH (n:$labels)
                                  WHERE n.merged = True
                                  WITH n LIMIT $limit
                                  SET n.merged = Null
                           RETURN COUNT(*)',
                           {limit:$limit})
                       '''

        return Query(query_str=query_str,
                     template_string_parameters={
                         "labels": node_constructor.get_label_string(),
                     },
                     parameters={"limit": batch_size})

    @staticmethod
    def get_merge_nodes_with_same_id_query(node_constructor: NodeConstructor, batch_size: int):
        if "Event" in node_constructor.get_labels():
            return None


        query_str = '''
                    CALL apoc.periodic.commit(
                           'MATCH (n:$labels)
                           WHERE n.merged IS NULL
                           // we order by sysId
                           WITH n ORDER BY n.sysId LIMIT $limit
                           WITH collect(n) as collection
                           WITH collection, last(collection) as last_node, size(collection) as collection_size
                           UNWIND collection as n
                           WITH $idt_properties, collect(n) as same_nodes, last_node, collection_size
                           CALL {WITH same_nodes, last_node
                                MATCH (last_node)
                                // last node could be the first of the list of same_nodes, we do not set to merged = true
                                // for this node
                                 WHERE size(same_nodes) = 1 and  head(same_nodes) <> last_node
                                 SET head(same_nodes).merged = True}             
                           WITH same_nodes, collection_size                   
                           WHERE size(same_nodes) > 1
                           UNWIND range(0, toInteger(floor(size(same_nodes)/100))) as i
                           WITH same_nodes, i*100 as min_range, apoc.coll.min([(i+1)*100, size(same_nodes)]) AS 
                           max_range, collection_size
                           WITH same_nodes[min_range..max_range] as lim_nodes, collection_size
                           CALL apoc.refactor.mergeNodes(lim_nodes, {properties: "discard", mergeRels: true})
                           YIELD node
                           RETURN collection_size',
                           {limit:$limit})
                       '''


        return Query(query_str=query_str,
                     template_string_parameters={
                         "labels": node_constructor.get_label_string(),
                         "idt_properties": node_constructor.get_idt_properties_query()
                     },
                     parameters={"limit": batch_size})



    @staticmethod
    def get_infer_corr_from_parent_query(batch_size, relation_constructor, use_from):
        if use_from:
            node =  relation_constructor.from_node.get_pattern()
            from_or_to = "FROM"
        else:
            node  = relation_constructor.to_node.get_pattern()
            from_or_to = "TO"

        query_str = '''
            CALL apoc.periodic.commit('
                MATCH (e:Event) --> ($node) - [:$from_or_to] - (relation:$relation_label_str)
                WHERE NOT EXISTS ((e) - [:CORR] -> (relation))
                WITH DISTINCT relation, e limit $limit
                MERGE (e) - [:CORR] -> (relation)
                RETURN COUNT(*)',
                {limit:$limit}
                )       
            '''

        return Query(query_str=query_str,
                     template_string_parameters={
                         "node": node,
                         "from_or_to": from_or_to,
                         "relation_label_str": relation_constructor.result.get_relation_types_str()
                     },
                     parameters={
                         "limit": batch_size
                     })



    @staticmethod
    def get_create_relation_by_relations_query(relation_constructor: RelationConstructor, batch_size: int) -> Query:
        if relation_constructor.model_as_node:
            # language=sql
            merge_str = '''
                            MERGE ($from_node_name) -[:FROM] -> (relation:$relation_label_str) - [:TO] -> (
                            $to_node_name)
                            '''
        else:
            merge_str = "MERGE ($from_node_name) -[$rel_pattern] -> ($to_node_name)"

        # language=SQL
        query_str = '''
                CALL apoc.periodic.iterate(
                '$relation_queries                        
                RETURN distinct $from_node_name, $to_node_name',
                '$merge_str',                        
                {batchSize: $batch_size})
            '''

        query_str = Template(query_str).safe_substitute({
            "merge_str": merge_str
        })

        return Query(query_str=query_str,
                     template_string_parameters={
                         "relation_queries": relation_constructor.get_relations_query(),
                         "from_node_name": relation_constructor.from_node.get_name(),
                         "to_node_name": relation_constructor.to_node.get_name(),
                         "rel_pattern": relation_constructor.result.get_pattern(),
                         "relation_label_str": relation_constructor.result.get_relation_types_str()
                     },
                     parameters={
                         "batch_size": batch_size
                     })

    @staticmethod
    def get_create_relation_using_record_query(relation_constructor: RelationConstructor, batch_size: int) -> Query:
        # find events that are related to different entities of which one event also has a reference to the other entity
        # create a relation between these two entities
        if relation_constructor.model_as_node:
            # language=sql
            merge_str = '''
                            MERGE ($from_node_name) -[:FROM] -> (relation:$relation_label_str) - [:TO] -> (
                            $to_node_name)
                            MERGE (relation)  - [:PREVALENCE] -> (record)
                            '''
        else:
            merge_str = "MERGE ($from_node_name) -[$rel_pattern] -> ($to_node_name)"

        query_str = '''     CALL apoc.periodic.commit('
                            MATCH (record:$record_labels)
                            WHERE NOT record:RecordCreated
                            WITH  record limit $limit
                            MATCH ($from_node) - [:PREVALENCE] -> (record)
                            MATCH ($to_node) - [:PREVALENCE] -> (record)
                            $merge_str
                            SET record:RecordCreated
                            RETURN COUNT(*)',
                            {limit:$limit})
                        '''

        query_str = Template(query_str).safe_substitute({
            "merge_str": merge_str
        })

        return Query(query_str=query_str,
                     template_string_parameters={
                         "from_node": relation_constructor.from_node.get_pattern(),
                         "from_node_name": relation_constructor.from_node.get_name(),
                         "to_node": relation_constructor.to_node.get_pattern(),
                         "to_node_name": relation_constructor.to_node.get_name(),
                         "record_labels": relation_constructor.prevalent_record.get_label_str(
                             include_first_colon=False),
                         "rel_pattern": relation_constructor.result.get_pattern("relation"),
                         "relation_labels": relation_constructor.result.get_relation_types_str(as_list=True),
                         "relation_label_str": relation_constructor.result.get_relation_types_str()
                     },
                     parameters={
                         "limit": batch_size
                     })

    @staticmethod
    def get_reset_created_relation_query(relation_constructor: RelationConstructor, batch_size: int) -> Query:
        # find events that are related to different entities of which one event also has a reference to the other entity
        # create a relation between these two entities

        query_str = '''     CALL apoc.periodic.commit('
                              MATCH (record:$record_labels)
                              WHERE record.rel_created = True
                              WITH record limit $limit
                              SET record.rel_created = Null
                              RETURN COUNT(*)',
                              {limit:$limit})
                          '''

        return Query(query_str=query_str,
                     template_string_parameters={
                         "record_labels": relation_constructor.prevalent_record.get_label_str(
                             include_first_colon=False),
                         "limit": batch_size
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
                        'MATCH (n:$entity_labels_string) <-[:$corr_type_string]- (e:$event_label)
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
                                        'MATCH (n:$entity_labels_string) <-[:$corr_type_string]- (e:$event_label)
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
                                'MATCH (n:$entity_labels_string) <-[:$corr_type_string]- (e:$event_label)
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
                         "corr_type_string": entity.get_corr_type_strings(),
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
