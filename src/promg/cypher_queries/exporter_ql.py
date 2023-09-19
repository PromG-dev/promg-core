from ..data_managers.semantic_header import ConstructedNodes
from ..database_managers.db_connection import Query


class ExporterQueryLibrary:

    @staticmethod
    def get_event_log_query(entity: ConstructedNodes, additional_event_attributes, additional_entity_attributes) -> Query:
        query_str = '''
                MATCH (e:Event) - [:$corr_type] -> (n:$node_label)
                RETURN n.sysId as caseId, e.activity as activity, e.timestamp as timestamp $extra_attributes $entity_attributes
                ORDER BY n.sysId, e.timestamp
            '''

        attributes_query = ",".join(f"e.{attribute} as {attribute}" for attribute in additional_event_attributes)
        entity_attributes_query = ",".join(f"n.{attribute} as {attribute}" for attribute in additional_entity_attributes)
        return Query(query_str=query_str,
                     template_string_parameters={
                         "node_label": entity.get_label_string(),
                         "corr_type": entity.get_corr_type_strings(),
                         "extra_attributes": f", {attributes_query}" if len(additional_event_attributes) > 0 else "",
                         "entity_attributes": f", {entity_attributes_query}" if len(additional_entity_attributes) > 0 else ""
                     })
