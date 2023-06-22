from dataclasses import dataclass
from typing import Dict, Optional, Any, List
import re

from ..data_managers.datastructures import DataStructure
from ..data_managers.semantic_header import ConstructedNodes, Relation, NodeConstructor, Relationship, Node
from ..database_managers.db_connection import Query
from string import Template


class CypherQueryLibrary:
















    @staticmethod
    def get_make_timestamp_date_query(attribute, datetime_object, batch_size) -> Query:
        """
        Convert the strings of the timestamp to the datetime as used in cypher
        Remove the str_timestamp property
        @return: None
        """
        offset = datetime_object.timezone_offset
        offset = f'{attribute}+"{offset}"' if offset != "" else attribute

        # language=SQL
        query_str = '''
            CALL apoc.periodic.iterate(
            'MATCH (e:Event) WHERE e.$attribute IS NOT NULL AND e.justImported = True 
            WITH e, e.$offset as timezone_dt
            WITH e, datetime(apoc.date.convertFormat(timezone_dt, "$datetime_object_format", 
                "$datetime_object_convert_to")) as converted
            RETURN e, converted',
            'SET e.$attribute = converted',
            {batchSize:$batch_size, parallel:false})
        '''

        return Query(query_str=query_str,
                     template_string_parameters={
                         "datetime_object_format": datetime_object.format,
                         "datetime_object_convert_to": datetime_object.convert_to,
                         "batch_size": batch_size,
                         "attribute": attribute,
                         "offset": offset
                     })

    @staticmethod
    def get_convert_epoch_to_timestamp(attribute, datetime_object, batch_size):

        # language=SQL
        query_str = '''
            CALL apoc.periodic.iterate(
            'MATCH (e:Event) WHERE e.$attribute IS NOT NULL AND e.justImported = True 
            WITH e, e.$attribute as timezone_dt
            WITH e, apoc.date.format(timezone_dt, $unit, 
                $dt_format) as converted
            RETURN e, converted',
            'SET e.$attribute = converted',
            {batchSize:$batch_size, parallel:false, 
            params: {unit: $unit,
                    dt_format: $datetime_object_format}})
        '''

        return Query(query_str=query_str,
                     template_string_parameters={"attribute": attribute},
                     parameters={
                         "batch_size": batch_size,
                         "unit": datetime_object.unit,
                         "datetime_object_format": datetime_object.format
                     })

    @staticmethod
    def get_finalize_import_events_query(labels, batch_size) -> Query:
        labels = ":".join(labels)
        # language=SQL
        query_str = '''
        CALL apoc.periodic.iterate(
            'MATCH (e:$labels) 
            WHERE e.justImported = True 
            RETURN e',
            'REMOVE e.justImported',
            {batchSize:$batch_size, parallel:false})
        '''

        return Query(query_str=query_str,
                     template_string_parameters={"labels": labels},
                     parameters={"batch_size": batch_size})

    @staticmethod
    def get_filter_events_by_property_query(prop: str, values: Optional[List[str]] = None, exclude=True) -> Query:
        if values is None:  # match all events that have a specific property
            negation = "NOT" if exclude else ""
            # query to delete all events and its relationship with property
            # language=SQL
            query_str = '''
                MATCH (e:Event {justImported: True})
                WHERE e.$prop IS $negation NULL
                DETACH DELETE e
                '''
            template_string_parameters = {"prop": prop, "negation": negation}
        else:  # match all events with specific property and value
            negation = "" if exclude else "NOT"
            # match all e and delete them and its relationship
            # language=SQL
            query_str = '''
                MATCH (e:Event {justImported: true})
                WHERE $negation e.$prop IN $values
                DETACH DELETE e
            '''
            template_string_parameters = {"prop": prop, "negation": negation, "values": values}

        # execute query
        return Query(query_str=query_str,
                     template_string_parameters=template_string_parameters)

    @staticmethod
    def get_create_log_query() -> Query:
        # language=SQL
        query_str = ''' 
            MATCH (e:Event) WHERE e.log IS NOT NULL AND e.log <> "nan"
            WITH e.log AS log
            MERGE (:Log {ID:log})
        '''
        return Query(query_str=query_str)

    @staticmethod
    def get_link_events_to_log_query(batch_size) -> Query:
        # language=SQL
        query_str = '''
            CALL apoc.periodic.iterate(
                'MATCH (l:Log) 
                MATCH (e:Event {log: l.ID})
                RETURN e, l', 
                'MERGE (l)-[:HAS]->(e)',
                {batchSize:$batch_size})
        '''
        return Query(query_str=query_str,
                     parameters={"batch_size": batch_size})

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
        if node_constructor.infer_prevalence_record:
            # language=SQL
            query_str += '''MERGE (record) <- [:PREVALENCE] - ($result_node_name)
            '''
        if node_constructor.infer_corr_from_event_record:
            # language=SQL
            query_str += '''WITH record, $result_node_name
                            MATCH (event:Event) - [:PREVALENCE] -> (record) <- [:PREVALENCE] - ($result_node_name)
                            MERGE (event) - [:CORR] -> ($result_node_name)'''
        elif node_constructor.infer_observed:
            # language=SQL
            query_str += '''WITH record, $result_node_name
                            MATCH (event:Event) - [:PREVALENCE] -> (record) <- [:PREVALENCE] - ($result_node_name)
                            MERGE (event) <- [:OBSERVED] - ($result_node_name)'''

        return Query(query_str=query_str,
                     template_string_parameters={
                         "record": node_constructor.get_prevalent_record_pattern(node_name="record"),
                         "conditions": node_constructor.get_where_condition(node_name="record"),
                         "result_node": node_constructor.result.get_pattern(),
                         "result_node_name": node_constructor.result.get_name(),
                     })

    @staticmethod
    def get_create_relation_by_relations_query(relation: Relation, batch_size: int):
        # language=SQL
        query_str = '''
            CALL apoc.periodic.iterate(
            '$antecedents_query                        
            RETURN distinct $from_node, $to_node',
            'MERGE ($from_node) - [:$type $properties] -> ($to_node)',                        
            {batchSize: $batch_size})
        '''

        relation_constructor = relation.constructed_by
        from_node_name = relation_constructor.get_from_node_name()
        to_node_name = relation_constructor.get_to_node_name()

        properties = ""

        return Query(query_str=query_str,
                     template_string_parameters={
                         "antecedents_query": relation_constructor.get_antecedent_query(),
                         "from_node": from_node_name,
                         "to_node": to_node_name,
                         "type": relation.type,
                         "properties": properties,
                         "batch_size": batch_size
                     })

    @staticmethod
    def get_create_relation_using_nodes_query(relation: Relation) -> Query:
        # find events that are related to different entities of which one event also has a reference to the other entity
        # create a relation between these two entities

        query_str = '''
                        MATCH ($from_node) - [:PREVALENCE] -> (r)
                        MATCH ($to_node) - [:PREVALENCE] -> (r)
                        MATCH ($record_node)
                        MERGE ($from_node_name) -[$rel_pattern] -> ($to_node_name)
                    '''

        return Query(query_str=query_str,
                     template_string_parameters={
                         "from_node": relation.constructed_by.from_node.get_pattern(),
                         "from_node_name": relation.constructed_by.from_node.get_name(),
                         "to_node": relation.constructed_by.to_node.get_pattern(),
                         "to_node_name": relation.constructed_by.to_node.get_name(),
                         "record_node": relation.constructed_by.prevalent_record.get_pattern(name="r"),
                         "rel_pattern": relation.result.get_pattern()
                     })

    @staticmethod
    def get_create_entities_by_relations_query(node_constructor: NodeConstructor) -> Query:

        if node_constructor.infer_reified_relation:
            # language=sql
            query_str = '''
                            MATCH ($from_node) - [r:$rel_type] -> ($to_node)
                            MERGE ($from_node_name) <- [:REIFIED] - ($result_node) - [:REIFIED] -> ($to_node_name)
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

        #TODO from node
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
    def get_dfc_label(entity_type: str, include_label_in_dfc: bool) -> str:
        if include_label_in_dfc:
            return f'DF_C_{entity_type.upper()}'
        else:
            return f'DF_C'

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

    @staticmethod
    def get_aggregate_df_relations_query(entity: ConstructedNodes,
                                         include_label_in_c_df: bool = True,
                                         classifiers: Optional[List[str]] = None, df_threshold: int = 0,
                                         relative_df_threshold: float = 0,
                                         exclude_self_loops=True) -> Query:

        # add relations between classes when desired
        if df_threshold == 0 and relative_df_threshold == 0:
            # corresponds to aggregate_df_relations &  aggregate_df_relations_for_entities in graphdb-event-logs
            # aggregate only for a specific entity type and event classifier

            # language=sql
            query_str = '''
                            MATCH 
                            (c1:Class) <-[:OBSERVED]-(e1:Event) 
                                -[df:$df_label {entityType: '$entity_type'}]-> 
                            (e2:Event) -[:OBSERVED]-> (c2:Class)
                            MATCH (e1) -[:CORR] -> (n) <-[:CORR]- (e2)
                            WHERE n.entityType = df.entityType AND 
                                c1.classType = c2.classType $classifier_condition $classifier_self_loops
                            WITH n.entityType as EType,c1,count(df) AS df_freq,c2
                            MERGE 
                            (c1) 
                                -[rel2:$dfc_label {entityType: '$entity_type', type:'DF_C', classType: c1.classType}]-> 
                            (c2) 
                            ON CREATE SET rel2.count=df_freq'''
        else:
            # aggregate only for a specific entity type and event classifier
            # include only edges with a minimum threshold, drop weak edges (similar to heuristics miner)

            # language=sql
            query_str = '''
                            MATCH 
                            (c1:Class) 
                                <-[:OBSERVED]-
                            (e1:Event) 
                                -[df:$df_label {entityType: '$entity_type'}]-> 
                            (e2:Event) -[:OBSERVED]-> (c2:Class)
                            MATCH (e1) -[:CORR] -> (n) <-[:CORR]- (e2)
                            WHERE n.entityType = df.entityType 
                                AND c1.classType = c2.classType $classifier_condition $classifier_self_loops
                            WITH n.entityType as entityType,c1,count(df) AS df_freq,c2
                            WHERE df_freq > $df_threshold
                            OPTIONAL MATCH (c2:Class) <-[:OBSERVED]- (e2b:Event) -[df2:DF]-> 
                                (e1b:Event) -[:OBSERVED]-> (c1:Class)
                            WITH entityType as EType,c1,df_freq,count(df2) AS df_freq2,c2
                            WHERE (df_freq*$relative_df_threshold > df_freq2)
                            MERGE 
                            (c1) 
                                -[rel2:$dfc_label {entityType: '$entity_type', type:'DF_C', classType: c1.classType}]-> 
                            (c2) 
                            ON CREATE SET rel2.count=df_freq'''

        classifier_condition = ""
        if classifiers is not None:
            classifier_string = "_".join(classifiers)
            classifier_condition = f"AND c1.classType = '{classifier_string}'"

        return Query(query_str=query_str,
                     template_string_parameters={
                         "df_label": entity.get_df_label(),
                         "entity_type": entity.node_type,
                         "classifier_condition": classifier_condition,
                         "classifier_self_loops": "AND c1 <> c2" if exclude_self_loops else "",
                         "dfc_label": CypherQueryLibrary.get_dfc_label(entity.node_type, include_label_in_c_df),
                         "df_threshold": df_threshold,
                         "relative_df_threshold": relative_df_threshold
                     })

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
    def get_event_log(entity: ConstructedNodes, additional_event_attributes):

        query_str = '''
            MATCH (e:Event) - [:CORR] -> (n:$entity_label)
            RETURN n.ID as caseId, e.activity as activity, e.timestamp as timestamp $extra_attributes
            ORDER BY n.ID, e.timestamp
        '''

        attributes_query = ",".join(f"e.{attribute} as {attribute}" for attribute in additional_event_attributes)
        return Query(query_str=query_str,
                     template_string_parameters={
                         "entity_label": entity.get_label_string(),
                         "extra_attributes": f", {attributes_query}"
                     })

    @staticmethod
    def merge_same_nodes(data_structure: DataStructure):
        # language=sql
        query_str = '''
            MATCH (n:$labels)
            WITH $primary_keys, collect(n) as nodes
            CALL apoc.refactor.mergeNodes(nodes, {
                properties:'combine'})
            YIELD node
            RETURN node
        '''

        return Query(query_str=query_str,
                     template_string_parameters={
                         "labels": data_structure.get_label_string(),
                         "primary_keys": data_structure.get_primary_keys_as_attributes()
                     })

    @staticmethod
    def add_attributes_to_classifier(relation, label, properties, copy_as):
        # TODO align with relation and entity
        if copy_as is None:
            copy_as = properties

        # language=sql
        query_str = '''
                    MATCH (c:Class) - [:$relation] - (n:$label)
                    SET $properties
                '''
        properties = [f"c.{copy} = n.{property}" for (property, copy) in zip(properties, copy_as)]
        properties = ",".join(properties)

        return Query(query_str=query_str,
                     template_string_parameters={
                         "relation": relation,
                         "label": label,
                         "properties": properties
                     })

    @staticmethod
    def get_query_infer_items_propagate_upwards_multiple_levels(entity: ConstructedNodes, is_load=True) -> Query:
        # language=sql
        query_str = '''
            MATCH (f2:Event) - [:CORR] -> (n:$entity)
            MATCH (f2) - [:CORR] ->  (equipment:Equipment)
            MATCH (f2) - [:OBSERVED] -> (a2:Activity) - [:AT] -> (l:Location) - [:PART_OF*0..] -> (k:Location) 
            WITH f2, k, equipment, n
            CALL {WITH f2, k, equipment
                MATCH (f0:Event) - [:OBSERVED] -> (a0: Activity)
                MATCH (a0) - [:$operation_type] -> (et:EntityType {name: '$entity'})  
                MATCH (a0) - [:AT] -> (k)
                MATCH (f0) - [:CORR] ->  (equipment)
                WHERE f0.timestamp $comparison f2.timestamp
                RETURN f0 as f0_first
                ORDER BY f0.timestamp $order_type
                LIMIT 1}
            MERGE (f0_first) - [:CORR] -> (n)
            '''

        return Query(query_str=query_str,
                     template_string_parameters={
                         "entity": entity.node_type,
                         "operation_type": "LOADS" if is_load else "UNLOADS",
                         "comparison": "<=" if is_load else ">=",
                         "order_type": "DESC" if is_load else ""
                     })

    @staticmethod
    def get_query_infer_items_propagate_downwards_multiple_level_w_batching(entity: ConstructedNodes,
                                                                            relative_position: ConstructedNodes) -> Query:
        # language=sql
        query_str = '''
            MATCH (f2:Event) - [:CORR] -> (bp:$relative_position)
            MATCH (f2) - [:CORR] -> (equipment :Equipment)
            MATCH (f2) - [:OBSERVED] -> (a2:Activity) -[:AT]-> (l:Location) - [:PART_OF*0..] -> (k:Location) 
            // ensure f2 should have operated on the required by checking that the activity operates on that entity
            MATCH (a2) - -> (:EntityType {name: '$entity'}) 
            WITH f2, equipment, k, bp
            CALL {WITH f2, equipment, k
                MATCH (f0: Event)-[:OBSERVED]->(a0:Activity) - [:LOADS] ->  (:EntityType {name: '$entity'})
                MATCH (a0) - [:AT] -> (k)
                MATCH (f0)-[:CORR]->(resource)
                WHERE f0.timestamp <= f2.timestamp
                // find the first preceding f0
                RETURN f0 as f0_first_prec
                ORDER BY f0.timestamp DESC
                LIMIT 1
            }
            // only merge when f0_first_prec is actually related to the required entity
            WITH f2, [(f0_first_prec)-[:CORR]->(n:$entity)- [:AT_POS] -> (bp) | n] as related_n
            FOREACH (n in related_n | 
                MERGE (f2) - [:CORR] -> (n)
            )
        '''

        return Query(query_str=query_str,
                     template_string_parameters={
                         "entity": entity.node_type,
                         "relative_position": relative_position.node_type
                     })

    @staticmethod
    def get_query_infer_items_propagate_downwards_one_level(entity: ConstructedNodes) -> Query:
        # language=sql
        query_str = '''
                    MATCH (f1 :Event) - [:CORR] -> (equipment :Equipment)
                    MATCH (f1) - [:OBSERVED] -> (a1:Activity) -[:AT]-> (l:Location)
                    // ensure f2 should have operated on the required by checking that the activity operates on that  
                    entity
                    MATCH (a1) - -> (:EntityType {name: '$entity'}) 
                    WITH f1, equipment, l
                    CALL {WITH f1, equipment, l
                        MATCH (f0: Event)-[:OBSERVED]->(a0:Activity) - [:LOADS] -> (:EntityType {name: '$entity'})
                        MATCH (a0) - [:AT] -> (l)
                        MATCH (f0)-[:CORR]->(equipment)
                        WHERE f0.timestamp <= f1.timestamp
                        // find the first preceding f0
                        RETURN f0 as f0_first_prec
                        ORDER BY f0.timestamp DESC
                        LIMIT 1
                    }
                    // only merge when f0_first_prec is actually related to a Box
                    WITH f1, [(f0_first_prec)-[:CORR]->(n:$entity) | n] as related_n
                    FOREACH (n in related_n | 
                        MERGE (f1) - [:CORR] -> (n)
                    )
                    '''

        return Query(query_str=query_str,
                     template_string_parameters={
                         "entity": entity.node_type
                     })

    @staticmethod
    def match_entity_with_batch_position(entity: ConstructedNodes, relative_position: ConstructedNodes):
        # language=sql
        query_str = '''
                MATCH (e:Event) - [:CORR] -> (b:Box)
                MATCH (e) - [:CORR] -> (bp:$relative_position)
                MERGE (b:Box) - [:AT_POS] -> (bp:$relative_position)
            '''

        query_str = Template(query_str).substitute(entity=entity.node_type)
        return Query(query_str=query_str,
                     template_string_parameters={
                         "entity": entity.node_type,
                         "relative_position": relative_position.node_type
                     })
