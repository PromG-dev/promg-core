# PromG Library

`PromG` collects queries for modeling, importing, enriching and analyzing event data as Event Knowledge Graphs (EKGs).
The queries are run against a Neo4j instance. 

All scripts and queries are licensed under LGPL v3.0, see LICENSE.
Copyright information is provided within each Project.

This site contains the project documentation for `PromG`.

## Table Of Contents

1. [Tutorials](tutorials.md)
2. [How-To Guides](how-to-guides.md)
3. [Reference](reference.md)
4. [Explanation](explanation.md)

Quickly find what you're looking for depending on
your use case by looking at the different pages.

## Get started
### PromG
The library can be installed in Pyhton using pip
`pip install promg`.

### Neo4j
The library assumes that Neo4j is installed.

Install [Neo4j](https://neo4j.com/download/):

- Use the [Neo4j Desktop](https://neo4j.com/download-center/#desktop)  (recommended), or
- [Neo4j Community Server](https://neo4j.com/download-center/#community)

### Create a new graph database

- The scripts in this release assume password "12345678".
- The scripts assume the server to be available at the default URL `bolt://localhost:7687`
  - You can modify this also in the script.
- ensure to allocate enough memory to your database, advised: `dbms.memory.heap.max_size=5G`
- the script expects the `Neo4j APOC library` to be installed as a plugin, see https://neo4j.com/labs/apoc/


For example projects that use this library, have a look at [EKG BPI Challenges](https://github.com/Ava-S/ekg_bpi_challenges), [EKG Inferring missing identifiers](https://github.com/Ava-S/ekg_inference) and [EKG for AutoTwin EU GA n. 101092021.](https://github.com/Ava-S/ekg_autotwin)


## Projects/Modules

The following projects are part of this repository

### Event Knowledge Graphs

We use Event Knowledge Graphs as basis for our data model as they already naturally model Events, Activities, Entities and their relations for Process Mining.
The EKGs are stored in a labeled property graph in [Neo4J](https://neo4j.com/).

#### Publications
- Stefan Esser, Dirk Fahland: Multi-Dimensional Event Data in Graph
  Databases. [CoRR abs/2005.14552](https://arxiv.org/abs/2005.14552), [Journal on Data Semantics, DOI: 10.1007/s13740-021-00122-1](https://dx.doi.org/10.1007/s13740-021-00122-1) (
  2020)
- Esser, Stefan. (2020, February 19). A Schema Framework for Graph Event Data. Master thesis. Eindhoven University of
  Technology. https://doi.org/10.5281/zenodo.3820037


### OCED-PG
We developed a reference implementation for Object-Centric Event Data [OCED](https://icpmconference.org/2022/program/xes-symposium/). OCED-PG is a declarative extract-load-transformT framework, that maps the raw data to a corresponding EKG, using the semantic header as a basis.

We proposed a three-layer approach to create a semantic-aware representation and storage system for OCED. 

- _Base ontology_: The OCED proposal is formalized as a PG-schema \[1\] providing a common interface for process querying. The schema defines a _base ontology_ for representing and transforming OCED, which includes a _semantic layer_ (defining the OCED concepts) and a _record layer_ (defining concepts for generic data records from a legacy system and how they are related to the semantic layer). 

- _Reference ontology_: The base ontology is specialized into a domain-specific reference ontology using PG-schema's inheritance mechanism. The reference ontology includes a _semantic layer_ (defining the domain's semantic objects, events, and relations), and a _record layer_ (defining in which legacy records the domain-level concepts are stored). The structural definitions are extended with rules to transform data in the record layer into nodes and relationships of the semantic layer, similar to [ontology-based data access](https://doi.org/10.24963/ijcai.2018/777).
- _OCED-PG: declarative extract-load-transform (ELT) framework_. OCED-PG load the legacy data records into the graph DB as a record layer. We then transform the data records into OCED by automatically translating the transformation rules of step (2) into queries over the record layer.

### Multi-process Discovery and Analysis

#### Publications

### Task Identification

#### Publications

### Custom Modules

### Missing Case Identifiers Inference
Method to infer missing case identifiers in event data by exploiting knowledge about the activities and their locations.

#### Publications

## Scripts

### Main script
There is one script (orchestrator) that is used by applications to create an Event Knowledge graph. This script makes use of the library.

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




