from string import Template

from ..database_managers.db_connection import Query


class TaskIdentifierLibrary:
    @staticmethod
    def get_task_identifier_query() -> Query:
        # language = SQL
        query_str = ""
        return Query(query_str=query_str,
                     template_string_parameters={},
                     parameters={})
