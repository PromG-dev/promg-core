from string import Template
from typing import Union, Optional, List

from ..data_managers.semantic_header import ConstructedNodes, NodeConstructor, Node, \
    RelationConstructor, RecordConstructor, ConstructedRelation
from ..database_managers.db_connection import Query


class SemanticHeaderQueryLibrary:
    @staticmethod
    def get_create_node_by_record_constructor_query(node_constructor: NodeConstructor, merge=True,
                                                    logs: Optional[List[str]] = None) -> Query:
        # find events that contain the entity as property and not nan
        # save the value of the entity property as id and also whether it is a virtual entity
        # create a new entity node if it not exists yet with properties
        merge_or_create = 'MERGE' if merge else 'CREATE'
        set_label_str = ""
        set_property_str = node_constructor.get_set_result_properties_query()
        infer_corr_str = ""
        infer_observed_str = ""

        if node_constructor.set_labels is not None:
            set_label_str = f'SET $set_labels'''

        if len(node_constructor.inferred_relationships) > 0:
            infer_corr_str = '''WITH record, $result_node_name'''
            for relationship in node_constructor.inferred_relationships:
                infer_rel_str = '''
                    CALL {WITH record, $result_node_name
                            MATCH ($event_node) - [:EXTRACTED_FROM] -> (record:$record_labels) <- [:EXTRACTED_FROM] - (
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
        elif node_constructor.infer_observed:
            # language=SQL
            infer_observed_str = '''
            WITH record, $result_node_name
                                MATCH (event:$event_label) - [:EXTRACTED_FROM] -> (record) <- [:EXTRACTED_FROM] - (
                                $result_node_name)
                                CREATE (event) <- [:OBSERVED] - ($result_node_name)
                                '''

        # add check to only transform records from the imported logs
        if logs is not None:
            log_str = ",".join([f'"{log}"' for log in logs])
            log_check_str = f"WHERE record.log in [{log_str}]"
        else:
            log_check_str = ""

        # language=SQL
        query_str = '''
                    CALL apoc.periodic.iterate(
                    'MATCH ($record) 
                          $log_check_str
                          RETURN record',
                          '$merge_or_create ($result_node)
                          $set_label_str
                          $set_property_str
                          MERGE (record) <- [:EXTRACTED_FROM] - ($result_node_name)
                          $infer_corr_str
                          $infer_observed_str', {batchSize:$batch_size})
                    '''

        query_str = Template(query_str).safe_substitute({
            "set_label_str": set_label_str,
            "set_property_str": set_property_str,
            "infer_corr_str": infer_corr_str,
            "infer_observed_str": infer_observed_str,
            "merge_or_create": merge_or_create,
            "log_check_str": log_check_str
        })

        return Query(query_str=query_str,
                     template_string_parameters={
                         "record": node_constructor.get_prevalent_record_pattern(node_name="record"),
                         "record_name": "record",
                         "result_node": node_constructor.result.get_pattern(),
                         "result_node_name": node_constructor.result.get_name(),
                         "set_result_properties": node_constructor.get_set_result_properties_query(),
                         "set_labels": node_constructor.get_set_result_labels_query(),
                         "corr_type": node_constructor.corr_type,
                         "event_label": node_constructor.event_label
                     })

    @staticmethod
    def get_associated_record_labels_query(logs):
        log_str = ",".join([f'"{log}"' for log in logs])
        log_str = f"[{log_str}]"

        query_str = '''
            MATCH (r:Record)
            WHERE r.log in $log_str
            UNWIND labels(r) as _label
            RETURN collect(distinct _label) as labels
        '''

        return Query(query_str=query_str,
                     template_string_parameters={"log_str": log_str})

    @staticmethod
    def get_number_of_records_query(node_constructor: NodeConstructor):

        query_str = '''MATCH ($record) 
                           RETURN count(record) as count'''

        return Query(query_str=query_str,
                     template_string_parameters={
                         "record": node_constructor.get_prevalent_record_pattern(node_name="record")
                     })

    @staticmethod
    def get_infer_corr_from_parent_query(relation_constructor, use_from):
        if use_from:
            node = relation_constructor.from_node.get_pattern()
            from_or_to = "FROM"
        else:
            node = relation_constructor.to_node.get_pattern()
            from_or_to = "TO"

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
                     })

    @staticmethod
    def get_create_relation_using_record_query(relation_constructor: RelationConstructor,
                                               logs: Optional[List[str]] = None) -> Query:
        # find events that are related to different entities of which one event also has a reference to the other entity
        # create a relation between these two entities
        if relation_constructor.model_as_node:
            # language=sql
            merge_str = '''
                            MERGE ($from_node_name) -[:FROM] -> (relation:$relation_label_str) - [:TO] -> (
                            $to_node_name)
                            MERGE (relation)  - [:EXTRACTED_FROM] -> (record)
                            '''
        else:
            merge_str = "MERGE ($from_node_name) -[$rel_pattern] -> ($to_node_name)"

        # add check to only transform records from the imported logs
        if logs is not None:
            log_str = ",".join([f'"{log}"' for log in logs])
            log_check_str = f"WHERE record.log in [{log_str}]"
        else:
            log_check_str = ""

        query_str = '''     CALL apoc.periodic.iterate('
                            MATCH (record:$record_labels)
                            $log_check_str
                            RETURN record',
                            '
                            MATCH ($from_node) - [:EXTRACTED_FROM] -> (record)
                            MATCH ($to_node) - [:EXTRACTED_FROM] -> (record)
                            $merge_str',
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
                         "record_labels": relation_constructor.prevalent_record.get_label_str(
                             include_first_colon=False),
                         "rel_pattern": relation_constructor.result.get_pattern("relation"),
                         "relation_labels": relation_constructor.result.get_relation_types_str(as_list=True),
                         "relation_label_str": relation_constructor.result.get_relation_types_str()
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
                         "entity_type": node.type,
                         "df_entity": node.get_df_label()
                     })
