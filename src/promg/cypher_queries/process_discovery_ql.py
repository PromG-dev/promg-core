from typing import Optional, List
from ..data_managers.semantic_header import ConstructedNodes
from ..database_managers.db_connection import Query


class AnalysisQueryLibrary:

    @staticmethod
    def get_aggregate_df_relations_query(entity: ConstructedNodes,
                                         include_label_in_df_a: bool = True,
                                         df_threshold: int = 0,
                                         relative_df_threshold: float = 0,
                                         exclude_self_loops=True) -> Query:

        # add relations between classes when desired
        if df_threshold == 0 and relative_df_threshold == 0:
            # corresponds to aggregate_df_relations &  aggregate_df_relations_for_entities in graphdb-event-logs
            # aggregate only for a specific entity type and event classifier

            # language=sql
            query_str = '''
                                MATCH 
                                (c1:Activity) -[:OBSERVED]-> (e1:Event) 
                                    -[df:$df_label {entityType: '$entity_type'}]-> 
                                (e2:Event) <-  [:OBSERVED] - (c2:Activity)
                                $classifier_self_loops
                                WITH c1, count(df) AS df_freq,c2
                                MERGE 
                                (c1) 
                                    -[rel2:$dfc_label {entityType: '$entity_type', type:'DF_A'}]-> 
                                (c2) 
                                ON CREATE SET rel2.count=df_freq'''
        else:
            # aggregate only for a specific entity type and event classifier
            # include only edges with a minimum threshold, drop weak edges (similar to heuristics miner)

            # language=sql
            query_str = '''
                                MATCH 
                                (c1:Activity) 
                                    -[:OBSERVED]->
                                (e1:Event) 
                                    -[df:$df_label {entityType: '$entity_type'}]-> 
                                (e2:Event) <-[:OBSERVED]- (c2:Activity)
                                MATCH (e1) -[:CORR] -> (n) <-[:CORR]- (e2)
                                $classifier_self_loops
                                WITH c1,count(df) AS df_freq,c2
                                WHERE df_freq > $df_threshold
                                OPTIONAL MATCH (c2:Activity) -[:OBSERVED]-> (e2b:Event) -[df2:$df_label {entityType: 
                                '$entity_type'}]-> 
                                    (e1b:Event) <-[:OBSERVED]- (c2:Activity)
                                WITH c1,df_freq,count(df2) AS df_freq2,c2
                                WHERE (df_freq*$relative_df_threshold > df_freq2)
                                MERGE 
                                (c1) 
                                    -[rel2:$dfc_label {entityType: '$entity_type', type:'DF_A'}]-> 
                                (c2) 
                                ON CREATE SET rel2.count=df_freq'''

        return Query(query_str=query_str,
                     template_string_parameters={
                         "df_label": entity.get_df_label(),
                         "entity_type": entity.type,
                         "classifier_self_loops": "WHERE c1 <> c2" if exclude_self_loops else "",
                         "dfc_label": entity.get_df_a_label(include_label_in_df_a),
                         "df_threshold": df_threshold,
                         "relative_df_threshold": relative_df_threshold
                     })
