from promg import DatabaseConnection
from promg.database_managers.db_connection_provider import DatabaseConnectionProviderInterface


class DatabaseConnectionProviderImpl(DatabaseConnectionProviderInterface):
    #https://python-dependency-injector.ets-labs.org/introduction/di_in_python.html
    def __init__(self):
        super().__init__()

    def get_connection(self) -> DatabaseConnection:
        # contract, altijd dezelfde, of verschilende, maximum aantal??
        raise NotImplementedError

    def close_connection(self, connection: DatabaseConnection) -> bool:
        # returns whether connection is successfully closed
        raise NotImplementedError

    def close_all_connections(self) -> bool:
        # returns whether connection is successfully closed
        raise NotImplementedError
