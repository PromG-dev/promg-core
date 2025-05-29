from string import Template
from typing import Optional, Dict


class Query:
    def __init__(self, query_str: str, database: str = None, parameters: Optional[Dict[str, any]] = None,
                 template_string_parameters: Optional[Dict[str, any]] = None):
        if template_string_parameters is not None:
            self.query_string = Template(query_str).safe_substitute(template_string_parameters)
        else:
            self.query_string = query_str
        self.kwargs = parameters
        self.database = database
