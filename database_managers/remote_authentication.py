# remote credentials --> STORE REMOTE CREDENTIALS SOMEWHERE DIFFERENT (not pushed to github)
from database_managers.authentication import Credentials

remote = Credentials(
    uri="[your_uri]",
    user="neo4j",
    password="[your_password]"
)