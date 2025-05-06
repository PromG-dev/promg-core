from string import Template
from typing import Optional, List, Dict, Any, Tuple

import neo4j
from ..utilities.configuration import Configuration


class Query:
    def __init__(self, query_str: str, database: str = None, parameters: Optional[Dict[str, any]] = None,
                 template_string_parameters: Optional[Dict[str, any]] = None):
        if template_string_parameters is not None:
            self.query_string = Template(query_str).safe_substitute(template_string_parameters)
        else:
            self.query_string = query_str
        self.kwargs = parameters
        self.database = database


class Driver(object):
    def __init__(self, uri, auth):
        self._driver = neo4j.GraphDatabase.driver(uri=uri, auth=auth, max_connection_lifetime=200)

    def get_session(self, database):
        return self._driver.session(database=database)


class DatabaseConnection:
    def __init__(self, uri: str, db_name: str, user: str, password: str, verbose: bool = False,
                 batch_size: int = 100000):
        self.db_name = db_name
        self.verbose = verbose
        self.batch_size = batch_size
        self.driver = Driver(uri=uri, auth=(user, password))

    def exec_query(self, function, **kwargs):
        # check whether connection can be made
        result = function(**kwargs)
        if result is None:
            return
        query = result.query_string
        kwargs = result.kwargs
        database = result.database
        if kwargs is None:
            kwargs = {}  # replace None value by an emtpy dictionary
        if ("$batch_size" in query
                and "batch_size" not in kwargs):  # ensure to not override batch_size if already defined
            kwargs["batch_size"] = self.batch_size
        if ("$limit" in query
                and "limit" not in kwargs):  # ensure to not override limit if already defined
            kwargs["limit"] = self.batch_size

        if "apoc.periodic.commit" in query:
            limit = kwargs["limit"]
            failed_batches = 1
            attempts = 0
            while failed_batches > 0 and attempts <= 10:
                result = self._exec_query(query, database, **kwargs)
                failed_batches = result[0]['failedBatches']
                kwargs["batch_size"] = int(limit / 2)
                kwargs["batch_size"] = max(10000, kwargs["batch_size"])
                attempts += 1
            if failed_batches > 0:
                raise Exception(f"Maximum attempts reached: {result[0]['batchErrors']}")

            return result
        else:
            return self._exec_query(query, database, **kwargs)

    def _exec_query(self, query: str, database: str = None, **kwargs) -> Optional[List[Dict[str, Any]]]:
        """
        Write a transaction of the query to  the server and return the result
        @param query: string, query to be executed
        @param database: string, Name of the database
        @return: The result of the query or None
        """

        def run_query(tx: neo4j.Transaction, _query: str, **_kwargs) -> Tuple[
            Optional[List[Dict[str, Any]]], neo4j.ResultSummary]:

            """
                Run the query and return the result of the query
                @param tx: transaction class on which we can perform queries to the database
                @param _query: string
                @return: The result of the query or None if there is no result
            """
            # get the results after the query is executed
            _result = tx.run(_query, _kwargs)
            _result_records = _result.data()  # obtain dict representation
            _summary = _result.consume()  # exhaust the result

            # or empty list
            return _result_records, _summary

        if self.verbose:
            print(query)

        if database is None:
            database = self.db_name

        with self.driver.get_session(database=database) as session:
            try:  # try to commit the transaction, if the transaction fails, it is rolled back automatically
                result, summary = session.execute_write(run_query, query, **kwargs)
                return result
            except Exception as inst:  # let user know the transaction failed and close the connection
                print("Latest transaction was rolled back")
                print(f"This was your latest query: {query}")
                print(inst)

    @staticmethod
    def set_up_connection(config: Configuration):
        return DatabaseConnection(db_name=config.user, uri=config.uri, user=config.user,
                                  password=config.password, verbose=config.verbose, batch_size=config.batch_size)
