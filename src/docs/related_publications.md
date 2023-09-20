
The following projects are part of this repository

## Event Knowledge Graphs

We use Event Knowledge Graphs as basis for our data model as they already naturally model Events, Activities, Entities and their relations for Process Mining.
The EKGs are stored in a labeled property graph in [Neo4J](https://neo4j.com/).

### Publications
- `Stefan Esser, Dirk Fahland: Multi-Dimensional Event Data in Graph
  Databases. [CoRR abs/2005.14552](https://arxiv.org/abs/2005.14552), [Journal on Data Semantics, DOI: 10.1007/s13740-021-00122-1](https://dx.doi.org/10.1007/s13740-021-00122-1) (
  2020)`
- `Esser, Stefan. (2020, February 19). A Schema Framework for Graph Event Data. Master thesis. Eindhoven University of
  Technology. https://doi.org/10.5281/zenodo.3820037`


## OCED-PG
We developed a reference implementation for Object-Centric Event Data [OCED](https://icpmconference.org/2022/program/xes-symposium/). OCED-PG is a declarative extract-load-transformT framework, that maps the raw data to a corresponding EKG, using the semantic header as a basis.

We proposed a three-layer approach to create a semantic-aware representation and storage system for OCED. 

- _Base ontology_: The OCED proposal is formalized as a PG-schema \[1\] providing a common interface for process querying. The schema defines a _base ontology_ for representing and transforming OCED, which includes a _semantic layer_ (defining the OCED concepts) and a _record layer_ (defining concepts for generic data records from a legacy system and how they are related to the semantic layer). 

- _Reference ontology_: The base ontology is specialized into a domain-specific reference ontology using PG-schema's inheritance mechanism. The reference ontology includes a _semantic layer_ (defining the domain's semantic objects, events, and relations), and a _record layer_ (defining in which legacy records the domain-level concepts are stored). The structural definitions are extended with rules to transform data in the record layer into nodes and relationships of the semantic layer, similar to [ontology-based data access](https://doi.org/10.24963/ijcai.2018/777).
- _OCED-PG: declarative extract-load-transform (ELT) framework_. OCED-PG load the legacy data records into the graph DB as a record layer. We then transform the data records into OCED by automatically translating the transformation rules of step (2) into queries over the record layer.

## Multi-process Discovery and Analysis

`D. Fahland, “Process mining over multiple behavioral dimensions with
event knowledge graphs,” in Process Mining Handbook. Springer, 2022,
vol. 448, pp. 274–319`

## Task Identification

`Klijn, E.L., Mannhardt, F., Fahland, D. (2021). Classifying and Detecting Task Executions and Routines in Processes Using Event Graphs. In: Polyvyanyy, A., Wynn, M.T., Van Looy, A., Reichert, M. (eds) Business Process Management Forum. BPM 2021. Lecture Notes in Business Information Processing, vol 427. Springer, Cham. https://doi.org/10.1007/978-3-030-85440-9_13`

## Inference of missing case identifiers

`Swevels, A., Dijkman, R., Fahland, D. (2023). Inferring Missing Entity Identifiers from Context Using Event Knowledge Graphs. In: Di Francescomarino, C., Burattin, A., Janiesch, C., Sadiq, S. (eds) Business Process Management. BPM 2023. Lecture Notes in Computer Science, vol 14159. Springer, Cham. https://doi.org/10.1007/978-3-031-41620-0_11`