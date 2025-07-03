import collections
from string import Template
from typing import Optional, List, Dict, Any, Tuple, Union
from typing import Callable  # type hint for function returns

import neo4j

from .custom_exceptions import BatchQueryExecutionError
from .models.query import Query
from ..utilities.configuration import Configuration

QueryResult = Optional[List[Dict[str, Any]]]
QueryMapper = Callable[..., Query]
QueryStr = str


class Driver(object):
    def __init__(self, uri, auth):
        self._driver = neo4j.GraphDatabase.driver(uri=uri, auth=auth, max_connection_lifetime=200)

    def get_session(self, database):
        return self._driver.session(database=database)


class DatabaseConnection:
    def __init__(self, uri: str, db_name: str, user: str, password: str, verbose: bool = False,
                 batch_size: int = 100_000):
        self.db_name = db_name
        self.verbose = verbose
        self.batch_size = batch_size
        self.driver = Driver(uri=uri, auth=(user, password))

    def exec_query(self, query: Union[Query, QueryStr, QueryMapper], **kwargs) -> QueryResult:
        transformed_query = self._transform_query(query, **kwargs)
        return self._dispatch_query(query=transformed_query)

    @staticmethod
    def _transform_query(query: Union[Query, QueryStr, QueryMapper], **kwargs) -> Query:
        if isinstance(query, Query):
            transformed_query = query
        elif isinstance(query, QueryStr):
            transformed_query = Query(query_str=query)
        elif isinstance(query, collections.abc.Callable):
            transformed_query = query(**kwargs)  # run function that returns Query object

            if not isinstance(transformed_query, Query):  # Type checker expects Query return
                raise TypeError(f"Expected function to return Query, got {type(transformed_query)}")
        else:
            raise TypeError(f'Unsupported query type: {type(query)}')

        return transformed_query

    def _prepare_query(self, query: Query) -> Tuple[str, Dict[str, Any], str, bool, bool]:
        """
        Normalizes a Query object by filling in default parameters and checking if batching is required.

        @param query: A Query object containing the query string, parameters, and database name.

        @return: A tuple containing:
            - query_str (str): The Cypher query string.
            - kwargs (Dict[str, Any]): The processed query parameters.
            - db_name (str): The name of the database to run the query against.
            - is_batched (bool): True if the query uses batching (e.g., via apoc.periodic.commit), False otherwise.
            - is_implicit (bool): True if the query is an implicit query as indicated by the flag :auto, False otherwise.
        """

        # Unpack Query Object
        query_str = query.query_string
        kwargs = query.kwargs or {}  # replace None value by an emtpy dictionary
        db_name = query.database

        if "batch_size" not in kwargs:  # override batch_size if NOT already defined
            kwargs["batch_size"] = self.batch_size

        if "limit" not in kwargs:  # override limit if NOT already defined
            kwargs["limit"] = self.batch_size

        is_batched = "apoc.periodic.commit" in query_str or "apoc.periodic.iterate" in query_str

        is_implicit = query_str.strip().lower().startswith(":auto")
        if is_implicit:
            query_str = query_str.replace(":auto", "")

        return query_str, kwargs, db_name, is_batched, is_implicit

    def _dispatch_query(self, query: Query) -> QueryResult:
        query_str, query_kwargs, db_name, is_batched, is_implicit = self._prepare_query(query)

        if is_batched:
            limit = query_kwargs.pop("limit")
            return self._run_batched_query(query_str=query_str,
                                           limit=limit,
                                           db_name=db_name,
                                           **query_kwargs)
        else:
            return self._exec_query(query_str=query_str,
                                    db_name=db_name,
                                    is_implicit=is_implicit,
                                    **query_kwargs)

    def _run_batched_query(self, query_str: str, limit: int, db_name: str, **query_kwargs) -> QueryResult:
        failed_batches = 1
        attempts = 0
        result = None
        while failed_batches > 0 and attempts <= 10:
            result = self._exec_query(
                query_str=query_str,
                db_name=db_name,
                **query_kwargs)
            failed_batches = result[0]['failedBatches']
            query_kwargs["batch_size"] = int(limit / 2)
            query_kwargs["batch_size"] = max(10000, query_kwargs["batch_size"])
            attempts += 1
        if failed_batches > 0:
            if "iterate" in query_str:
                raise BatchQueryExecutionError(f"Maximum attempts reached: {result[0]['errorMessages']}")
            else:
                raise BatchQueryExecutionError(f"Maximum attempts reached: {result[0]['batchErrors']}")

        return result

    def _exec_query(self, query_str: str, db_name: str = None, is_implicit=False, **query_kwargs) -> QueryResult:
        """
        Write a transaction of the query to  the server and return the result
        @param query_str: string, query to be executed
        @param db_name: string, Name of the database
        @return: The result of the query or None
        """

        def run_query(tx: neo4j.Transaction, _query_str: str, **_query_kwargs) -> Tuple[QueryResult, neo4j.ResultSummary]:
            """
                Run the query and return the result of the query
                @param tx: transaction class on which we can perform queries to the database
                @param _query_str: string
                @return: The result of the query or None if there is no result
            """
            # get the results after the query is executed
            _result = tx.run(
                query=_query_str,  # I'm aware that this should be a LiteralString, however I cannot enforce this.
                parameters=_query_kwargs
            )
            _result_records = _result.data()  # obtain dict representation
            _summary = _result.consume()  # exhaust the result

            # or empty list
            return _result_records, _summary

        if self.verbose:
            print(f"Executing query in {'IMPLICIT' if is_implicit else 'EXPLICIT'} mode.")
            print(f"Database: {db_name}")
            print(f"Query:\n{query_str}")
            if query_kwargs:
                print(f"Parameters: {query_kwargs}")

        if db_name is None:
            db_name = self.db_name

        with self.driver.get_session(database=db_name) as session:
            try:  # try to commit the transaction, if the transaction fails, it is rolled back automatically
                if is_implicit:
                    result = session.run(query_str, **query_kwargs).data()
                else:
                    result, summary = session.execute_write(run_query, query_str, **query_kwargs)
                return result
            except Exception as inst:  # let user know the transaction failed and close the connection
                print("Latest transaction was rolled back")
                print(f"This was your latest query: {query_str}")
                print(inst)

    @staticmethod
    def set_up_connection(config: Configuration):
        return DatabaseConnection(db_name=config.user, uri=config.uri, user=config.user,
                                  password=config.password, verbose=config.verbose, batch_size=config.batch_size)
