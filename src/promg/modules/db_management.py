from typing import List, Set, Optional, Dict

from tabulate import tabulate

from ..cypher_queries.db_managment_ql import DBManagementQueryLibrary as dbm_ql
from ..utilities.performance_handling import Performance


class DBManagement:
    def __init__(self, db_connection, semantic_header=None):
        self.connection = db_connection
        self.semantic_header = semantic_header

    @Performance.track()
    def clear_db(self, replace=True) -> bool:
        """
        Replace or clear the entire database by a new one

        Args:
            replace: boolean to indicate whether the database is fully replaced

        """
        if replace:
            result = self.connection.exec_query(dbm_ql.get_replace_db_query, **{"db_name": self.connection.db_name})
            if result[0]['state'] == 'CaughtUp' and result[0]['success']:
                return True
            else:
                return False
        else:
            self.connection.exec_query(dbm_ql.get_delete_relationships_query)
            self.connection.exec_query(dbm_ql.get_delete_nodes_query)
            return True

    @Performance.track()
    def set_constraints(self, entity_key_name="sysId") -> None:
        """
        Set constraints in Neo4j instance
        """
        self._set_identifier_constraints(entity_key_name=entity_key_name)
        self.connection.exec_query(dbm_ql.get_set_unique_log_name_index_query)
        self.connection.exec_query(dbm_ql.get_set_activity_index_query)
        self.connection.exec_query(dbm_ql.get_set_record_id_as_range_query)
        self.connection.exec_query(dbm_ql.get_set_record_type_range_query)

    def get_constraints(self, ignore_defaults=True):
        results = self.connection.exec_query(dbm_ql.get_constraints_query)
        constraint_names = [result['name'] for result in results]
        if ignore_defaults:
            constraint_names.remove("index_343aff4e")  # default token lookup index for node labels
            constraint_names.remove("index_f7700477")  # default token lookup index for relationship types
        return constraint_names

    def _set_identifier_constraints(self, entity_key_name="sysId"):
        if self.semantic_header is not None:
            for node in self.semantic_header.nodes:
                # set the unique constraint per entity type node
                node_labels = node.get_labels(as_str=False)
                node_type = node.type

                identifier_properties = node.get_identifier_properties()
                if len(identifier_properties) > 0:
                    self.connection.exec_query(dbm_ql.get_set_identifier_index_query,
                                               **{
                                                   "node_type": node.type,
                                                   "identifier_properties": identifier_properties
                                               })

                elif "Entity" in node_labels:
                    self.connection.exec_query(dbm_ql.get_constraint_unique_entity_uid_query,
                                               **{
                                                   "node_type": node_type,
                                                   "entity_key_name": entity_key_name
                                               })
        else:
            # is semantic header is not defined, we just set sysid as range (instead of uniqueness constraint)
            self.connection.exec_query(dbm_ql.get_set_sysid_index_query,
                                       **{
                                           "entity_key_name": entity_key_name
                                       })

    def get_all_rel_types(self) -> List[str]:
        """
        Get all relationship types that are present in Neo4j instance

        Returns:
            A list of strings with all relationship types present in the Neo4j instance
        """

        # execute the query and store the result
        result = self.connection.exec_query(dbm_ql.get_all_rel_types_query)
        # in case there are no rel types, the result is None
        # return in this case an emtpy list
        if result is None:
            return []
        # store the results in a list
        result = [record["rel_type"] for record in result]
        return result

    def get_all_node_labels(self) -> Set[str]:
        """
        Get all node labels that are present in Neo4j instance

        Returns:
            A list of strings with all node labels present in the Neo4j instance
        """

        # execute the query and store the result
        result = self.connection.exec_query(dbm_ql.get_all_node_labels_query)
        # in case there are no labels, return an empty set
        if result is None:
            return set([])
        # some nodes have multiple labels, which are returned as a list of labels
        # therefore we need to flatten the result and take the set
        result = set([record for sublist in result for record in sublist["label"]])
        return result

    def get_statistics(self) -> List[Dict[str, any]]:
        """
        Get the count of nodes per label and the count of relationships per type

        Returns:
            A list containing dictionaries with the label/relationship and its count
        """

        def make_empty_list_if_none(_list: Optional[List[Dict[str, str]]]):
            if _list is not None:
                return _list
            else:
                return []

        node_count = self.connection.exec_query(dbm_ql.get_node_count_query)
        edge_count = self.connection.exec_query(dbm_ql.get_edge_count_query)
        agg_edge_count = self.connection.exec_query(dbm_ql.get_aggregated_edge_count_query)
        result = \
            make_empty_list_if_none(node_count) + \
            make_empty_list_if_none(edge_count) + \
            make_empty_list_if_none(agg_edge_count)
        return result

    def print_statistics(self) -> None:
        """
        Print the statistics nicely using tabulate
        """
        print(tabulate(self.get_statistics()))

    def get_imported_logs(self) -> List[str]:
        result = self.connection.exec_query(dbm_ql.get_imported_logs_query)
        return result[0]['logs']
