from promg import DatabaseConnection


class DatabaseConnectionProviderInterface:
    def __init__(self):
        pass

    def get_connection(self) -> DatabaseConnection:
        # contract, altijd dezelfde, of verschilende, maximum aantal??
        raise NotImplementedError

    def close_connection(self, connection: DatabaseConnection) -> bool:
        # returns whether connection is successfully closed
        raise NotImplementedError

    def close_all_connections(self) -> bool:
        # returns whether connection is successfully closed
        raise NotImplementedError
