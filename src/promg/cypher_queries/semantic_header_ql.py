from string import Template
from typing import Union, Optional, List

from ..data_managers.semantic_header import ConstructedNodes, NodeConstructor, Node, \
    RelationConstructor, RecordConstructor, ConstructedRelation
from ..database_managers.db_connection import Query


class SemanticHeaderQueryLibrary:
    @staticmethod
    def get_create_node_by_record_constructor_query(node_constructor: NodeConstructor, merge=True,
                                                    logs: Optional[List[str]] = None) -> Query:
        if merge:
            # for each result node, merge the node if it does not exist yet. Then merge it to the record node.
            # So, even though a Entity may appear in multiple records, it is only created once.
            merge_or_create_node = '''MERGE ($result_node)
                                 MERGE (record) <- [:EXTRACTED_FROM] - ($result_node_name)'''
        else:
            # for each result node, merge the node if it does not exist yet NEITHER does its relation to the record
            # node.
            # This ensures that for each record node, exactly one result node is created.
            # e.g. When creating (:Event) nodes, we create one (:Event) node for each (:EventRecord)
            merge_or_create_node = '''MERGE (record) <- [:EXTRACTED_FROM] - ($result_node)'''

        # get the string for optional properties
        set_property_str = node_constructor.get_set_result_properties_query()

        # in case some labels need to be set, we define the string
        set_label_str = ""
        if node_constructor.set_labels is not None:
            set_label_str = f'SET $set_labels'''

        # in case a correlation needs to be created, we define the string
        infer_corr_str = ""
        # in case multiple correlations can be inferred depending on the record types, we create a string for each
        # inference
        if len(node_constructor.inferred_relationships) > 0:
            infer_corr_str = '''WITH record, $result_node_name'''
            for relationship in node_constructor.inferred_relationships:
                infer_rel_str = '''
                    CALL {WITH record, $result_node_name
                            $record_match
                            MATCH ($event_node) - [:EXTRACTED_FROM] -> (record) <- [:EXTRACTED_FROM] - (
                                $result_node_name)
                                MERGE (event) - [:$relation_type] -> ($result_node_name)}'''
                infer_rel_str = Template(infer_rel_str).safe_substitute({
                    "event_node": relationship.event.get_pattern(name="event"),
                    "record_match": relationship.get_record_type_match(record_name="record"),
                    "relation_type": relationship.relation_type
                })
                infer_corr_str += infer_rel_str
        elif node_constructor.infer_corr_from_event_record:
            # only one correlation is created, create a string for this with the corr type
            # language=SQL
            infer_corr_str = '''
            WITH record, $result_node_name
                                MATCH (event:$event_label) - [:EXTRACTED_FROM] -> (record) <- [:EXTRACTED_FROM] - (
                                $result_node_name)
                                MERGE (event) - [:$corr_type] -> ($result_node_name)'''
        elif node_constructor.infer_corr_from_entity_record:  # TODO update such that only correct events are considered
            # language=SQL
            infer_corr_str = '''
                        WITH record, $result_node_name
                                MATCH (event:$event_label) - [:EXTRACTED_FROM] -> (record) <- [:EXTRACTED_FROM] - (
                                $result_node_name)
                                MERGE (event) - [:$corr_type] -> ($result_node_name)'''

        # in case an observed relations needs to be created, we define the string
        infer_observed_str = ""
        if node_constructor.infer_observed:
            # language=SQL
            infer_observed_str = '''
            WITH record, $result_node_name
                                MATCH (event:$event_label) - [:EXTRACTED_FROM] -> (record) <- [:EXTRACTED_FROM] - (
                                $result_node_name)
                                MERGE (event) <- [:OBSERVED] - ($result_node_name)
                                '''

        # add check to only transform records from the imported logs
        if logs is not None:
            log_str = ",".join([f'"{log}"' for log in logs])
            log_check_str = f"<- [:CONTAINS] - (log:Log) WHERE log.name in [{log_str}]"
        else:
            log_check_str = ""

        # create the overall query where we match the correct record nodes
        # then we create/merge the resulting node and set all labels, properties and inferred relations
        # language=SQL
        query_str = '''
                    CALL apoc.periodic.iterate(
                    'MATCH ($record) $log_check_str
                    $record_matches
                    // order records by elementId, this will determine the order in which events are created
                    // this is important for the temporal ordering of :Event nodes 
                    // when creating DF edges in case the timestamps are similar
                          RETURN record ORDER BY elementId(record)',
                          '$merge_or_create_node
                          $set_label_str
                          $set_property_str
                          $infer_corr_str
                          $infer_observed_str', {batchSize:$batch_size})
                    '''

        query_str = Template(query_str).safe_substitute({
            "set_label_str": set_label_str,
            "set_property_str": set_property_str,
            "infer_corr_str": infer_corr_str,
            "infer_observed_str": infer_observed_str,
            "merge_or_create_node": merge_or_create_node,
            "log_check_str": log_check_str
        })

        return Query(query_str=query_str,
                     template_string_parameters={
                         "record": node_constructor.get_prevalent_record_pattern(node_name="record"),
                         "record_matches": node_constructor.get_prevalent_match_record_pattern(node_name="record"),
                         "record_name": "record",
                         "result_node": node_constructor.result.get_pattern(),
                         "result_node_name": node_constructor.result.get_name(),
                         "set_result_properties": node_constructor.get_set_result_properties_query(),
                         "set_labels": node_constructor.get_set_result_labels_query(),
                         "corr_type": node_constructor.corr_type,
                         "event_label": node_constructor.event_label
                     })

    @staticmethod
    def get_associated_record_types_query(logs):
        log_str = ",".join([f'"{log}"' for log in logs])
        log_str = f"[{log_str}]"

        # request all associated record types for specific logs
        query_str = '''
            MATCH (record:Record) - [:IS_OF_TYPE] -> (record_type:RecordType)
            MATCH (record) <- [:CONTAINS] - (log:Log)
            WHERE log.name in $log_str
            RETURN collect(distinct record_type.type) as labels
        '''

        return Query(query_str=query_str,
                     template_string_parameters={"log_str": log_str})


    @staticmethod
    def get_infer_corr_from_parent_query(relation_constructor, use_from):
        if use_from:
            node = relation_constructor.from_node.get_pattern()
            from_or_to = "FROM"
        else:
            node = relation_constructor.to_node.get_pattern()
            from_or_to = "TO"

        # add correlation to a child node if its parent is correlated to an event
        query_str = '''
            CALL apoc.periodic.iterate('
                MATCH (e:Event) --> ($node) - [:$from_or_to] - (relation:$relation_label_str)
                WHERE NOT EXISTS ((e) - [:CORR] -> (relation))
                RETURN DISTINCT relation, e',
                'MERGE (e) - [:$corr_type] -> (relation)',
                {batchSize:$batch_size}
                )       
            '''

        return Query(query_str=query_str,
                     template_string_parameters={
                         "node": node,
                         "from_or_to": from_or_to,
                         "relation_label_str": relation_constructor.result.get_relation_types_str(),
                         "corr_type": relation_constructor.corr_type
                     })

    @staticmethod
    def get_create_relation_by_relations_query(relation_constructor: RelationConstructor) -> Query:
        if relation_constructor.model_as_node:
            # language=sql
            merge_str = '''
                            MERGE ($from_node_name) -[:FROM] -> (relation:$rel_pattern) - [:TO] -> (
                            $to_node_name)
                            '''
        else:
            merge_str = "MERGE ($from_node_name) -[$rel_pattern] -> ($to_node_name)"

        # language=SQL
        query_str = '''
                CALL apoc.periodic.iterate(
                '$relation_queries                        
                RETURN distinct $from_node_name, $to_node_name',
                '$merge_str
                $set_properties_str',                        
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
                         "set_properties_str": relation_constructor.get_set_result_properties_query()
                     })

    @staticmethod
    def get_create_relation_by_nodes_query(relation_constructor: RelationConstructor) -> Query:
        if relation_constructor.model_as_node:
            # language=sql
            merge_str = '''
                            MERGE ($from_node_name) -[:FROM] -> (relation:$rel_pattern) - [:TO] -> (
                            $to_node_name)
                            '''
        else:
            merge_str = "MERGE ($from_node_name) -[$rel_pattern] -> ($to_node_name)"

        # language=SQL
        query_str = '''
                    CALL apoc.periodic.iterate(
                    '$node_queries                        
                    RETURN distinct $from_node_name, $to_node_name',
                    '$merge_str
                    $set_properties_str',                        
                    {batchSize: $batch_size})
                '''

        query_str = Template(query_str).safe_substitute({
            "merge_str": merge_str
        })

        return Query(query_str=query_str,
                     template_string_parameters={
                         "node_queries": relation_constructor.get_node_query(),
                         "from_node_name": relation_constructor.from_node.get_name(),
                         "to_node_name": relation_constructor.to_node.get_name(),
                         "rel_pattern": relation_constructor.result.get_pattern(),
                         "set_properties_str": relation_constructor.get_set_result_properties_query()
                     })



    @staticmethod
    def get_create_relation_using_record_query(relation_constructor: RelationConstructor,
                                               logs: Optional[List[str]] = None) -> Query:
        # find events that are related to different entities of which one event also has a reference to the other entity
        # create a relation between these two entities
        if relation_constructor.model_as_node:
            # language=sql
            merge_str = '''
                            MERGE ($from_node_name) -[:FROM] -> (relation:$rel_pattern) - [:TO] -> (
                            $to_node_name)
                            MERGE (relation)  - [:EXTRACTED_FROM] -> (record)
                            '''
        else:
            merge_str = "MERGE ($from_node_name) -[$rel_pattern] -> ($to_node_name)"

        # add check to only transform records from the imported logs
        if logs is not None:
            log_str = ",".join([f'"{log}"' for log in logs])
            log_check_str = f"<- [:CONTAINS] - (log:Log) WHERE log.name in [{log_str}]"
        else:
            log_check_str = ""

        # match all records that are related to the correct record types and in specific logs
        # then match all from and to nodes that are extracted from these records
        # merge the resulting node
        # set the optional properties
        query_str = '''     CALL apoc.periodic.iterate('
                            MATCH ($record) $log_check_str
                            $record_matches
                            RETURN record',
                            '
                            MATCH ($from_node) - [:EXTRACTED_FROM] -> (record)
                            MATCH ($to_node) - [:EXTRACTED_FROM] -> (record)
                            $merge_str
                            $set_properties_str',
                            {batchSize:$batch_size})
                        '''

        query_str = Template(query_str).safe_substitute({
            "merge_str": merge_str,
            "log_check_str": log_check_str
        })

        return Query(query_str=query_str,
                     template_string_parameters={
                         "from_node": relation_constructor.from_node.get_pattern(),
                         "from_node_name": relation_constructor.from_node.get_name(),
                         "to_node": relation_constructor.to_node.get_pattern(),
                         "to_node_name": relation_constructor.to_node.get_name(),
                         "record": relation_constructor.get_prevalent_record_pattern(node_name="record"),
                         "record_matches": relation_constructor.get_prevalent_match_record_pattern(node_name="record"),
                         "rel_pattern": relation_constructor.result.get_pattern("relation"),
                         "relation_labels": relation_constructor.result.get_relation_types_str(as_list=True),
                         "set_properties_str": relation_constructor.get_set_result_properties_query("relation")
                     })

    @staticmethod
    def get_add_duration_query_str(add_duration) -> str:
        if add_duration:
            # check the type of the timestamp attributes.
            # DATE, DATETIME, TIME --> create Duration between first and second
            # INT, FLOAT --> save difference between second and first
            add_duration_str = '''
                , CASE 
                    WHEN apoc.meta.cypher.type(first.timestamp) IN ["DATE_TIME", "TIME", "DATE"] 
                        AND apoc.meta.cypher.type(second.timestamp) IN ["DATE_TIME", "TIME", "DATE"] 
                        THEN duration.between(first.timestamp, second.timestamp)
                    WHEN apoc.meta.cypher.type(first.timestamp) IN ["INTEGER", "FLOAT"]
                     AND apoc.meta.cypher.type(second.timestamp) IN ["INTEGER", "FLOAT"]
                     THEN  second.timestamp - first.timestamp
                    ELSE NULL
                END AS duration
            '''
        else:
            # we don't want to add the duration, so we use NULL for duration
            # This ensures us that we can use duration in the rest of the queries
            add_duration_str = '''
                , NULL as duration
            '''

        return add_duration_str

    @staticmethod
    def get_create_directly_follows_query(entity: Union[ConstructedNodes, ConstructedRelation], event_label,
                                          add_duration: bool = False) -> Query:
        # find the specific entities and events with a certain label correlated to that entity
        # order all events by time, order_nr and id grouped by a node n
        # collect the sorted nodes as a list
        # unwind the list from 0 to the one-to-last node
        # find neighbouring nodes and add an edge between

        # language=sql

        if event_label == "CompoundEvent":
            if entity.type == "Resource":
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
                        RETURN n, first, second $add_duration_str',
                        'MERGE (first) -[df:$df_entity {entityType: "$entity_type"}]->(second)
                         SET df.type = "DF"
                         SET df.entityId = n.sysId
                         SET df.duration = duration
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
                                        RETURN first, second $add_duration_str',
                                        'MERGE (first) -[df:$df_entity {entityType: "$entity_type"}]->(second)
                                         SET df.type = "DF"
                                         SET df.duration = duration
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
                                RETURN first, second $add_duration_str',
                                'MERGE (first) -[df:$df_entity {entityType: "$entity_type"}]->(second)
                                 SET df.type = "DF"
                                 SET df.duration = duration
                                ',
                                {batchSize: $batch_size})
                            '''

        return Query(query_str=query_str,
                     template_string_parameters={
                         "entity_labels_string": entity.get_label_string(),
                         "corr_type_string": entity.get_corr_type_strings(),
                         "event_label": event_label,
                         "df_entity": entity.get_df_label(),
                         "entity_type": entity.type,
                         "add_duration_str": SemanticHeaderQueryLibrary.get_add_duration_query_str(add_duration)
                     })

    @staticmethod
    def get_merge_duplicate_df_entity_query(node: ConstructedNodes) -> Query:

        # language=sql
        query_str = '''
                        MATCH (n1:Event)-[rel:$df_entity {entityType: '$entity_type'}]->(n2:Event)
                        WITH n1, n2, collect(rel) AS rels
                        WHERE size(rels) > 1
                        // only include this and the next line if you want to remove the existing relationships
                        UNWIND rels AS rel 
                        DELETE rel
                        MERGE (n1)
                            -[:$df_entity {entityType: '$entity_type', count:size(rels), type: 'DF'}]->
                              (n2)
                    '''
        return Query(query_str=query_str,
                     template_string_parameters={
                         "entity_type": node.type,
                         "df_entity": node.get_df_label()
                     })
