from typing import List, Set, Optional, Dict

from tabulate import tabulate

from ..cypher_queries.db_managment_ql import DBManagementQueryLibrary as dbm_ql
from ..database_managers.db_connection import DatabaseConnection
from ..utilities.performance_handling import Performance


class DBManagement:
    """
        Create DBManagement module
        Examples:
            >>> from promg.modules.db_management import DBManagement
            >>> db_manager = DBManagement()
    """

    def __init__(self):
        self.connection = DatabaseConnection()
        self.db_name = self.connection.db_name

    @Performance.track()
    def clear_db(self, replace: bool = True) -> None:
        """
        Replace or clear the entire database by a new one.

        Note:
            Note about difference between replacing and clearing.

            - Replace: results in an Empty database that is completely replaced with a new database.
                - Replacing a database is faster than clearing a database.
                - Only possible when
                    - you have an enterprise license
                    - you are running a local instance on the free desktop version

            - Clear: Results in an empty database, however constraints are still in place.
                - Clearing a database takes longer
                - Independent of license

        Args:
            replace: boolean to indicate whether the database may be replaced

        Examples:
            >>> db_manager.clear(replace=True)
            Results in an Empty database that is completely replaced with a new database
                (i.e. constraints are also removed)

            >>> db_manager.clear(replace=False)
            Results in an empty database, however constraints are still in place.
            Clearing an entire database takes longer.
        """
        if replace:
            self.connection.exec_query(dbm_ql.get_replace_db_query, **{"db_name": self.db_name})
        else:
            self.connection.exec_query(dbm_ql.get_delete_relationships_query)
            self.connection.exec_query(dbm_ql.get_delete_nodes_query)

    @Performance.track()
    def set_constraints(self) -> None:
        """
        Set constraints in Neo4j instance.

        - sysId property is used as index for (:Entity) nodes

        Examples:
            >>> db_managers.set_constraints()
            sysId is used as index for (:Entity) nodes
        """
        # # for implementation only (not required by schema or patterns)
        # self.connection.exec_query(dbm_ql.get_constraint_unique_event_id_query)
        #
        # required by core pattern
        # self.connection.exec_query(dbm_ql.get_constraint_unique_entity_uid_query)
        #
        # self.connection.exec_query(dbm_ql.get_constraint_unique_log_id_query)

        self.connection.exec_query(dbm_ql.get_set_sysid_index_query)

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
