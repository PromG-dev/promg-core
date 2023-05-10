# Using Graph Databases to Create an Event Knowledge Graph

## Description

This repository collects queries for modeling and importing incomplete event data as Event Knowledge Graphs using the Labeled
Property Graph data model of graph databases.
All scripts and queries are licensed under LGPL v3.0, see LICENSE.
Copyright information is provided within each Project.

## Requirements
This repository should be used as a submodule in another project to build an event knowledge graph.
To add the submodule to your project, run the following command in the root folder of your project.
`git submodule https://github.com/Ava-S/ekg_creator` 

For more information about submodule, have a look at [Git submodules](https://www.atlassian.com/git/tutorials/git-submodule).
For example projects that use this repository, have a look at [EKG BPI Challenges](https://github.com/Ava-S/ekg_bpi_challenges), [EKG Inferring missing identifiers](https://github.com/Ava-S/ekg_inference) and [EKG for AutoTwin EU GA n. 101092021.](https://github.com/Ava-S/ekg_autotwin)

Furthermore, we assume the following packages/databases to be installed.

### Neo4j
Install the neo4j-python-driver

`pip install neo4j`
OR
`conda install -c conda-forge neo4j-python-driver`

Install [Neo4j](https://neo4j.com/download/):

- Use the [Neo4j Desktop](https://neo4j.com/download-center/#desktop)  (recommended), or
- [Neo4j Community Server](https://neo4j.com/download-center/#community)

### Other packages
- `numpy`
- `pandas`
- `tabulate`
- `tqdm`

## Get started

### Create a new graph database

- The scripts in this release assume password "12345678".
- The scripts assume the server to be available at the default URL `bolt://localhost:7687`
  - You can modify this also in the script.
- ensure to allocate enough memory to your database, advised: `dbms.memory.heap.max_size=5G`
- the script expects the `Neo4j APOC library` to be installed as a plugin, see https://neo4j.com/labs/apoc/


## Projects

The following projects are part of this repository

### Missing Case Identifiers Inference
Method to infer missing case identifiers in event data by exploiting knowledge about the activities and their locations.

### Semantic Header (json files)
First version for semantic header for system/event knowledge graphs: https://multiprocessmining.org/2022/10/26/data-storage-vs-data-semantics-for-object-centric-event-data/

### Event Knowledge Graphs

Data model and generic query templates for translating and integrating a set of related CSV event logs into single event
graph over multiple behavioral dimensions, stored as labeled property graph in [Neo4J](https://neo4j.com/).
See [csv_to_eventgraph_neo4j/README.txt](ekg_creator/README.txt)

Publications:

- Stefan Esser, Dirk Fahland: Multi-Dimensional Event Data in Graph
  Databases. [CoRR abs/2005.14552](https://arxiv.org/abs/2005.14552), [Journal on Data Semantics, DOI: 10.1007/s13740-021-00122-1](https://dx.doi.org/10.1007/s13740-021-00122-1) (
  2020)
- Esser, Stefan. (2020, February 19). A Schema Framework for Graph Event Data. Master thesis. Eindhoven University of
  Technology. https://doi.org/10.5281/zenodo.3820037



## Scripts of submodule

### Main script
There is one script (orchestrator) that is used by applications to create an Event Knowledge graph. This script makes use of this submodule.

#### Data_managers

- data_managers/datastructures.py --> transforms the JSON file describing the different datasets into a class + additional methods
- data_managers/semantic_header.py --> transforms the JSON file describing the semantic header into a class + additional methods
- data_managers/interpreters.py --> Class that contains information about in what query language the semantic header and data structures should be interpreter


#### Database_managers
- database_managers/authentication.py  --> class containing the credentials to create connection to database. Local credentials are includes.
In case you want to create a remote connection, add the following piece of code to a (gitignored) file.
```python
remote = Credentials(
    uri="[your_uri]",
    user="neo4j",
    password="[your_password]"
)
```
- database_managers/db_connection.py --> class responsible for making the connection to the database and to communicate with the database
- database_managers/EventKnowledgeGraph.py --> class responsible for making (changes to) the EKG and to request data from the EKG. Makes use of several modules.

#### EKG_Modules
- ekg_modules/db_management.py --> general module to manage the database
- ekg_modules/data_importer.py --> imports the data stored in the records into the EKG
- ekg_modules/ekg_builder_semantic_header.py --> creates the required nodes and relations as specified in the semantic header
- ekg_modules/inference_engine.py --> module responsible for inferring missing information
- ekg_modules/ekg_analysis.py --> module for analysis of the EKG (e.g. create process model)
- ekg_modules/ekg_custom_module.py --> module to create custom queries, specific for this example

#### CypherQueries
Contains repeatable pieces of Cypher Queries for all necessary parts.
- cypher_queries/query_translators --> translate semantic header and data structures into Cypher
- cypher_queries/query_library --> contains all cypher queries for the EKG modules
- cypher_queries/custom_query_library --> contains all custom cypher queries for this example for the EKG modules




