from dataclasses import dataclass
from enum import Enum


# from remote_authentication import remote
# if you want a remote connection, store credentials somewhere different (e.g. remote_authentication)

@dataclass
class Credentials:
    uri: str
    user: str
    password: str


# local credentials
local = Credentials(
    uri="bolt://localhost:7687",
    user="neo4j",
    password="12345678"
)

remote = None


class Connections(Enum):
    REMOTE = 1
    LOCAL = 2


connections_map = {
    Connections.REMOTE: remote,
    Connections.LOCAL: local
}
