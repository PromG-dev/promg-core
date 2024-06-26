from typing import List, Set, Optional, Dict

from tabulate import tabulate

from ..cypher_queries.db_managment_ql import DBManagementQueryLibrary as dbm_ql
from ..utilities.performance_handling import Performance


class DBManagement:
    def __init__(self, db_connection, semantic_header=None):
        self.connection = db_connection
        self.semantic_header = semantic_header

    @Performance.track()
    def clear_db(self, replace=True) -> None:
        """
        Replace or clear the entire database by a new one

        Args:
            replace: boolean to indicate whether the database is fully replaced

        """
        if replace:
            result = self.connection.exec_query(dbm_ql.get_replace_db_query, **{"db_name": self.connection.db_name})
            self.set_constraints()
            if result[0]['state'] == 'CaughtUp' and result[0]['success']:
                return True
            else:
                return False
        else:
            self.connection.exec_query(dbm_ql.get_delete_relationships_query)
            return self.connection.exec_query(dbm_ql.get_delete_nodes_query)

    @Performance.track()
    def set_constraints(self) -> None:
        """
        Set constraints in Neo4j instance
        """
        # # for implementation only (not required by schema or patterns)
        # self.connection.exec_query(dbm_ql.get_constraint_unique_event_id_query)
        #
        # required by core pattern
        if self.semantic_header is not None:
            self._set_unique_sysid_constraints()
        else:
            self.connection.exec_query(dbm_ql.get_set_sysid_index_query)
        # self.connection.exec_query(dbm_ql.get_constraint_unique_log_id_query)

        self.connection.exec_query(dbm_ql.get_set_activity_index_query)
        self.connection.exec_query(dbm_ql.get_set_timestamp_event_index_query)
        self.connection.exec_query(dbm_ql.get_set_activity_event_index_query)
        self.connection.exec_query(dbm_ql.get_set_recordid_as_key_node_query)
        self.connection.exec_query(dbm_ql.get_set_recordid_as_index_query)
        self.connection.exec_query(dbm_ql.get_set_record_log_as_index_query)
        self.connection.exec_query(dbm_ql.get_set_record_created_as_index_query)
        self.connection.exec_query(dbm_ql.get_set_load_status_as_index_query)

    def _set_unique_sysid_constraints(self):
        for node in self.semantic_header.nodes:
            node_labels = node.get_labels(as_str=False)
            if "Entity" in node_labels:
                self.connection.exec_query(dbm_ql.get_constraint_unique_entity_uid_query,
                                           **{
                                               "node_type": node.type
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
        imported_logs = self.connection.exec_query(dbm_ql.get_imported_logs_query)
        return imported_logs
