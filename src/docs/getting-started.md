## PromG
The library can be installed in Pyhton using pip
`pip install promg`.

## Neo4j
The library assumes that Neo4j is installed.

Install [Neo4j](https://neo4j.com/download/):

- Use the [Neo4j Desktop](https://neo4j.com/download-center/#desktop)  (recommended), or
- [Neo4j Community Server](https://neo4j.com/download-center/#community)

## Create a new graph database

- The scripts in this release assume password "12345678".
- The scripts assume the server to be available at the default URL `bolt://localhost:7687`
  - You can modify this also in the script.
- ensure to allocate enough memory to your database, advised: `dbms.memory.heap.max_size=5G`
- the script expects the `Neo4j APOC library` to be installed as a plugin, see https://neo4j.com/labs/apoc/

## Ensure database is running

- When executing the main script, ensure that your database instance is running.