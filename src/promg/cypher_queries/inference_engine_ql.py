from ..data_managers.semantic_header import ConstructedNodes
from ..database_managers.db_connection import Query


class InferenceEngineQueryLibrary:
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
                         "entity": entity.type,
                         "operation_type": "LOADS" if is_load else "UNLOADS",
                         "comparison": "<=" if is_load else ">=",
                         "order_type": "DESC" if is_load else ""
                     })

    @staticmethod
    def get_query_infer_items_propagate_downwards_multiple_level_w_batching(entity: ConstructedNodes,
                                                                            relative_position: ConstructedNodes) -> \
            Query:
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
                         "entity": entity.type,
                         "relative_position": relative_position.type
                     })

    @staticmethod
    def get_query_infer_items_propagate_downwards_one_level(entity: ConstructedNodes) -> Query:
        # language=sql
        query_str = '''
                        MATCH (f1 :Event) - [:CORR] -> (equipment :Equipment)
                        MATCH (f1) - [:OBSERVED] -> (a1:Activity) -[:AT]-> (l:Location)
                        // ensure f2 should have operated on the required by checking that the activity operates on 
                        that  
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
                         "entity": entity.type
                     })

    @staticmethod
    def get_match_entity_with_batch_position_query(entity: ConstructedNodes, relative_position: ConstructedNodes) -> Query:
        # language=sql
        query_str = '''
                    MATCH (e:Event) - [:CORR] -> (b:Box)
                    MATCH (e) - [:CORR] -> (bp:$relative_position)
                    MERGE (b:Box) - [:AT_POS] -> (bp:$relative_position)
                '''

        return Query(query_str=query_str,
                     template_string_parameters={
                         "entity": entity.type,
                         "relative_position": relative_position.type
                     })
