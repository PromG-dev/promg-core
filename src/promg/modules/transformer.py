from typing import Optional, List, Union

from ..data_managers.semantic_header import ConstructedNodes, ConstructedRelation, Relationship, SemanticHeader, \
    NodeConstructor
from ..database_managers.db_connection import DatabaseConnection
from ..utilities.performance_handling import Performance
from ..cypher_queries.semantic_header_ql import SemanticHeaderQueryLibrary as sh_ql


class Transformer:
    """
        A module to transform records in the record layer/subgraphs of the semantic layer into a semantic layer
        using the semantic header

        - Nodes on the semantic layer are created based on records or a subgraph of the semantic layer
        - Relationships on the semantic layer are created based on records or a subgraph of the semantic layer

        Raises:
            ValueError when no semantic header has been built before.

        Examples:
            >>> from promg.modules.transformer import Transformer
            >>> from promg import SemanticHeader
            >>> semantic_header_path = Path(f'json_files/{dataset_name}.json')
            >>> semantic_header = SemanticHeader.create_semantic_header(semantic_header_path)
            >>> transformer = Transformer()
            Returns the transformer module
    """

    def __init__(self):
        self.connection = DatabaseConnection()
        self.semantic_header = SemanticHeader()

    def create_nodes_by_records(self, node_types: Optional[List[str]]) -> None:
        """
        Create nodes with node types using records.
        If no node types are defined, then all nodes types specified in the semantic header get created if they are
        created using records

        Args:
            node_types: An optional list of strings with the node types to be created
                        If the given node type is not constructed using a record, then it is ignored.

        Examples:
            Create nodes for a specific list of node types
            >>> transformer.create_nodes_by_records(["Event", "Activity", "Book"])
            The transformer creates the nodes with the types Event, Activity and Book
            as specified in semantic header in the Event Knowledge graph.

            Create nodes for all node types specified in the semantic header
            >>> transformer.create_nodes_by_records()
            The transformer creates the nodes that are constructed by a record
            as specified in the semantic header Event Knowledge graph.
        """
        for node_constructor in self.semantic_header.get_node_by_record_constructors(node_types):
            self._create_node_by_record(node_constructor=node_constructor)

    @Performance.track("node_constructor")
    def _create_node_by_record(self, node_constructor: NodeConstructor):
        """
        Create a node using a node constructor (which uses a (:Record) node)
        Args:
            node_constructor: Used constructor to build nodes

        Examples:
             >>> self._create_node_by_record(node_constructor=node_constructor)
        """

        # determine the number of ids to determine whether we merge first, we create and then merge or we just create
        num_ids = self.connection.exec_query(sh_ql.get_number_of_ids_query,
                                             **{
                                                 "node_constructor": node_constructor,
                                                 "use_record": True
                                             })
        # If number of ids < 1000, we first merge
        # except when the labels contains "Event" or "EntityAttribute" (then we always create)
        merge_first = num_ids[0]['num_ids'] < 1000 \
                      and "Event" not in node_constructor.get_labels() \
                      and "EntityAttribute" not in node_constructor.get_labels()

        # create the nodes using the node constructor and merge_first
        self.connection.exec_query(sh_ql.get_create_node_by_record_constructor_query,
                                   **{
                                       "node_constructor": node_constructor,
                                       "merge": merge_first
                                   })

        # reset the (:Record) nodes --> The used record nodes in previous query are adapted, so change them back to
        # original state
        self.connection.exec_query(sh_ql.get_reset_created_record_query)

        if merge_first:
            print(
                f"Node ({node_constructor.get_pattern(with_properties=False)})"
                f"using ({node_constructor.get_prevalent_record_pattern()}) merged")
        else:
            print(f"Node ({node_constructor.get_pattern(with_properties=False)}) "
                  f"using ({node_constructor.get_prevalent_record_pattern()}) "
                  f"created")
            if not ("Event" in node_constructor.get_labels() or "EntityAttribute" in node_constructor.get_labels()):
                # merge nodes if they are not merged yet (and have to be merged)
                self.connection.exec_query(sh_ql.get_merge_nodes_with_same_id_query,
                                           **{
                                               "node_constructor": node_constructor
                                           }
                                           )
                # reset the nodes for next merge iteration
                self.connection.exec_query(sh_ql.get_reset_merged_in_nodes_query,
                                           **{
                                               "node_constructor": node_constructor
                                           }
                                           )

    def _create_nodes_by_relations(self, node_types: Optional[List[str]]) -> None:
        """
        Create nodes with node types using subgraph of semantic layer (relations).
        If no node types are defined, then all nodes types specified in the semantic header get created if
        they are created using a subgraph

        Args:
            node_types: An optional list of strings with the node types to be created
                        If the given node type is not constructed using a subgraph, then it is ignored.

        Examples:
            Create nodes for a specific list of node types
            >>> transformer.create_nodes_by_relations(["BORROWED"])
            The transformer creates the nodes with the types BORROWED
            as specified in semantic header in the Event Knowledge graph.

            Create nodes for all node types specified in the semantic header
            >>> transformer.create_nodes_by_relations()
            The transformer creates the nodes that are constructed by a record
            as specified in the semantic header Event Knowledge graph.
        """
        for node_constructors in self.semantic_header.get_nodes_constructed_by_relations(node_types).values():
            for node_constructor in node_constructors:
                self._create_node_by_relation(node_constructor=node_constructor)

    @Performance.track("node_constructor")
    def _create_node_by_relation(self, node_constructor: NodeConstructor):
        pass

    def create_relations_using_records(self, relation_types: Optional[List[str]]) -> None:
        """
        Create relationships with relation types using records.
        That is two nodes related to the same (:Record) node get a relation in between as specified in the semantic
        header.
        If no relation types are defined, then all relation types specified in the semantic header get created if
        they are created using records

        Args:
            relation_types: An optional list of strings with the relation types to be created
                            If the given relation type is not constructed using a record, then it is ignored.

        Examples:
            Create nodes for a specific list of relation types
            >>> transformer.create_relations_using_records(["MEMBER_OF"])
            The transformer creates the relationships between two nodes with the type MEMBER_OF
            as specified in semantic header in the Event Knowledge graph.
            More specifically, a (:Member) node is related to a (:Library) node with a [:MEMBER_OF] relation
            when the (:Member) and (:Library) node are related to the same (:Record) node as specified in
            the semantic header

            Create nodes for all relationship types specified in the semantic header
            >>> transformer.create_relations_using_records()
            The transformer creates the relationships that are constructed by a subgraph
            as specified in the semantic header Event Knowledge graph.
        """

        # find events that are related to different entities of which one event also has a reference to the other entity
        # create a relation between these two entities
        relation: ConstructedRelation
        for relation_constructor in self.semantic_header.get_relations_constructed_by_record(relation_types):
            self._create_relations_using_record(relation_constructor=relation_constructor)

    @Performance.track("relation_constructor")
    def _create_relations_using_record(self, relation_constructor):
        """
        Create a relationships using a relation constructor (which uses a (:Record) node)
        Args:
            relation_constructor: Used constructor to build relationships

        Examples:
             >>> self._create_relations_using_record(relation_constructor=relation_constructor)
        """

        self.connection.exec_query(sh_ql.get_create_relation_using_record_query,
                                   **{
                                       "relation_constructor": relation_constructor
                                   })
        self.connection.exec_query(sh_ql.get_reset_created_record_query)
        self._create_corr_from_parents(relation_constructor=relation_constructor)

    def create_relations_using_relations(self, relation_types: Optional[List[str]]) -> None:
        """
        Create relations with relation types using subgraph of semantic layer (relations).
        If no relation types are defined, then all relation types specified in the semantic header get created if
        they are created using a subgraph

        Args:
            relation_types: An optional list of strings with the relation types to be created.
                            If the given relation type is not constructed using a subgraph, then it is ignored.

        Examples:
            Create nodes for a specific list of node types
            >>> transformer.create_relations_using_relations(["BORROWED"])
            The transformer creates the relationship with the types BORROWED
            as specified in semantic header in the Event Knowledge graph.

            Create nodes for all node types specified in the semantic header
            >>> transformer.create_relations_using_relations()
            The transformer creates the relationship that are constructed by a subgraph
            as specified in the semantic header Event Knowledge graph.
        """

        relation: ConstructedRelation
        for relation_constructor in self.semantic_header.get_relations_constructed_by_relations(relation_types):
            self._create_relations_using_relation(relation_constructor=relation_constructor)

    @Performance.track("relation_constructor")
    def _create_relations_using_relation(self, relation_constructor):
        """
        Create a relationships using a relation constructor (which uses a subgraph)
        Args:
            relation_constructor: Used constructor to build relationships

        Examples:
             >>> self._create_relations_using_relation(relation_constructor=relation_constructor)
        """
        self.connection.exec_query(sh_ql.get_create_relation_by_relations_query,
                                   **{
                                       "relation_constructor": relation_constructor
                                   })
        self._create_corr_from_parents(relation_constructor=relation_constructor)

    def _create_corr_from_parents(self, relation_constructor):
        """
        Add corr to a reified nodes using the parent nodes
        Args:
            relation_constructor: Constructor that was used to build the relationship.
                                  Used to determine the parents

        Examples:
             >>> self._create_corr_from_parents(relation_constructor=relation_constructor)
        """
        if relation_constructor.infer_corr_from_reified_parents:
            for use_from in [True, False]:
                self.connection.exec_query(sh_ql.get_infer_corr_from_parent_query,
                                           **{
                                               "relation_constructor": relation_constructor,
                                               "use_from": use_from
                                           })

    def create_df_edges(self, entity_types: List[str] = None, event_label: str = "Event") -> None:
        """
        For all nodes (nodes or reified relationship nodes) with entity_types create DF edges between (:Event) nodes
        matching event_label
        Args:
            entity_types: Optional list of types for which we want to create the DF edges.
                          If none, then we create DF edges for all nodes as specified in the semantic header.
                          If for the given entity types the infer_df is False (or not specified) in the semantic header,
                          then it is ignored (i.e. no DF edges are created)
            event_label: The label of the event nodes used to create the DF edges

        Examples:
             >>> self.create_df_edges(entity_types=["Book"], event_label="Event")
        """

        entity: ConstructedNodes

        if entity_types is None:
            entity_types = [entity.type for entity in self.semantic_header.nodes] \
                           + [relation.type for relation in self.semantic_header.relations if relation.model_as_node]

        for entity in self.semantic_header.nodes:
            if entity.infer_df and entity.type in entity_types:
                self._create_df_edges_for_entity(entity=entity, event_label=event_label)

        for relation in self.semantic_header.relations:
            if relation.model_as_node and relation.infer_df and relation.type in entity_types:
                self._create_df_edges_for_entity(entity=relation, event_label=event_label)

    @Performance.track("entity")
    def _create_df_edges_for_entity(self, entity: Union[ConstructedNodes, ConstructedRelation], event_label):
        """
        For node with specific entity create DF edges between event nodes with the correct event label
        Args:
            entity: The entity (either node or reified node) for which the DF edges need to be created
            event_label: The label of the event nodes used to create the DF edges

        Examples:
             >>> self._create_df_edges_for_entity(entity=entity, event_label="Event")
        """
        self.connection.exec_query(sh_ql.get_create_directly_follows_query,
                                   **{
                                       "entity": entity,
                                       "event_label": event_label
                                   })

    def merge_duplicate_df(self):
        """
        Merge duplicate DF edges between the same nodes
        Only do this for the (reified) nodes in semantic header with merge_duplicate_df = true
        """
        node: ConstructedNodes
        for node in self.semantic_header.nodes:
            if node.merge_duplicate_df:
                self._merge_duplicate_df_for_node(node=node)
        for relation in self.semantic_header.relations:
            if relation.model_as_node and relation.merge_duplicate_df:
                self._merge_duplicate_df_for_node(node=relation)


    @Performance.track("node")
    def _merge_duplicate_df_for_node(self, node: Union[ConstructedNodes, ConstructedRelation]):
        self.connection.exec_query(sh_ql.get_merge_duplicate_df_entity_query, **{"node": node})

    def _create_static_nodes_and_relations(self):
        print("No implementation yet")
        pass
