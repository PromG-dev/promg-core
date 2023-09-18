from ..data_managers.semantic_header import ConstructedNodes
from ..database_managers.db_connection import Query


class ExporterQueryLibrary:

    @staticmethod
    def get_event_log_query(entity: ConstructedNodes, additional_event_attributes) -> Query:
        query_str = '''
                MATCH (e:Event) - [:CORR] -> (n:$node_label)
                RETURN n.sysId as caseId, e.activity as activity, e.timestamp as timestamp $extra_attributes
                ORDER BY n.ID, e.timestamp
            '''

        attributes_query = ",".join(f"e.{attribute} as {attribute}" for attribute in additional_event_attributes)
        return Query(query_str=query_str,
                     template_string_parameters={
                         "node_label": entity.get_label_string(),
                         "extra_attributes": f", {attributes_query}" if len(additional_event_attributes) > 0 else ""
                     })
