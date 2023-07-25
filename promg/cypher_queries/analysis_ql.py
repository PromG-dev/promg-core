from typing import Optional, List
from promg.data_managers.semantic_header import ConstructedNodes
from promg.database_managers.db_connection import Query


class AnalysisQueryLibrary:
    @staticmethod
    def get_dfc_label(entity_type: str, include_label_in_dfc: bool) -> str:
        if include_label_in_dfc:
            return f'DF_C_{entity_type.upper()}'
        else:
            return f'DF_C'

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
                                    -[rel2:$dfc_label {entityType: '$entity_type', type:'DF_C', classType: 
                                    c1.classType}]-> 
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
                                    -[rel2:$dfc_label {entityType: '$entity_type', type:'DF_C', classType: 
                                    c1.classType}]-> 
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
                         "dfc_label": AnalysisQueryLibrary.get_dfc_label(entity.node_type, include_label_in_c_df),
                         "df_threshold": df_threshold,
                         "relative_df_threshold": relative_df_threshold
                     })
