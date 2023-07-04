import json
from abc import ABC
from pathlib import Path
from string import Template
from typing import List, Any, Optional, Union, Dict

from dataclasses import dataclass

from ..utilities.auxiliary_functions import replace_undefined_value, create_list, get_id_attribute_from_label
import re


class Property:
    def __init__(self, attribute: str, value: str, node_name: Optional[str],
                 node_attribute: Optional[str], ref_node: Optional[str], ref_attribute: Optional[str]):
        self.attribute = attribute
        self.value = value
        self.node_name = node_name,
        self.node_attribute = node_attribute,
        self.ref_node = ref_node
        self.ref_attribute = ref_attribute

    @staticmethod
    def from_string(property_description):
        if property_description is None:
            return None
        if ":" in property_description:
            components = property_description.split(":")
        else:
            components = property_description.split("=")
        attribute = components[0]
        value = components[1]
        attribute = attribute.strip()
        value = value.strip()

        ref_node, ref_attribute, node_name, node_attribute = None, None, None, None
        if "." in value:
            components = value.split(".")
            ref_node = components[0]
            ref_attribute = components[1]

        if "." in attribute:
            components = value.split(".")
            node_name = components[0]
            node_attribute = components[1]

        return Property(attribute=attribute, value=value, node_name=node_name,
                        node_attribute=node_attribute, ref_node=ref_node, ref_attribute=ref_attribute)

    def get_pattern(self, is_set=False):
        if not is_set:
            return f"{self.attribute}: {self.value}"
        else:
            return f"{self.attribute} = {self.value}"


@dataclass()
class Node:
    def __init__(self, name: str, labels: List[str], properties: List[Property], where_condition: str):
        self.name = name
        self.labels = labels
        self.properties = properties
        self.where_condition = where_condition

    @staticmethod
    def from_string(node_description: str) -> Optional["Node"]:
        if node_description is None:
            return None
        # we expect a node to be described in (node_name:Node_label)
        node_description = re.sub(r"[()]", "", node_description)
        node_components = node_description.split(":", 1)
        name = node_components[0]
        labels = ""
        where_condition = ""
        properties = []
        if len(node_components) > 1:
            node_labels_prop_where = node_components[1]
            node_labels_prop_where = node_labels_prop_where.replace("'", "\"")
            if "WHERE" in node_labels_prop_where:
                labels = node_labels_prop_where.split(" WHERE ")[0]
                where_condition = node_labels_prop_where.split(" WHERE ")[1]
            elif "{" in node_labels_prop_where:
                labels = node_labels_prop_where.split(" {")[0]
                properties = node_labels_prop_where.split(" {")[1]
                properties = properties.replace("}", "")
                properties = properties.split(",")
                properties = [Property.from_string(prop) for prop in properties]
            else:
                labels = node_labels_prop_where

        labels = labels.strip()
        labels = labels.split(":")

        return Node(name=name, labels=labels, properties=properties,
                    where_condition=where_condition)

    def get_name(self, with_brackets=False):
        if with_brackets:
            return f"({self.name})"
        else:
            return self.name

    def get_condition_string(self, with_brackets=False, with_where=False):
        if len(self.properties) > 0:
            return self._get_property_string(with_brackets)
        elif self.where_condition != "":
            return self._get_where_condition_string(with_where)
        else:
            return ""

    def _get_property_string(self, with_brackets=False):
        properties = ",".join([prop.get_pattern() for prop in self.properties])
        if with_brackets:
            property_string = "{$properties}"
            properties = Template(property_string).substitute(properties=properties)
        return properties

    def _get_where_condition_string(self, with_where=False):
        condition = self.where_condition
        if with_where:
            condition_string = "WHERE $where_condition"
            condition = Template(condition_string).substitute(where_condition=condition)
        return condition

    def get_pattern(self, name: Optional[str] = None, with_brackets=False, with_properties=True):

        node_pattern_str = "$node_name"
        if self.get_label_str() != "":
            node_pattern_str = "$node_name:$node_label"

        if name is None:
            node_pattern = Template(node_pattern_str).substitute(node_name=self.name,
                                                                 node_label=self.get_label_str())
        else:
            node_pattern = Template(node_pattern_str).substitute(node_name=name,
                                                                 node_label=self.get_label_str())
        if with_properties:
            node_pattern_str = "$node_pattern $condition_string"
            node_pattern = Template(node_pattern_str).substitute(node_pattern=node_pattern,
                                                                 condition_string=self.get_condition_string(
                                                                     with_brackets=True, with_where=True))
        if with_brackets:
            node_pattern_str = "($node_pattern)"
            node_pattern = Template(node_pattern_str).substitute(node_pattern=node_pattern)

        return node_pattern

    def get_label_str(self, include_first_colon=False):
        if len(self.labels) > 0:
            return ":" * include_first_colon + ":".join(self.labels)
        return ""

    def __repr__(self):
        return self.get_pattern(with_brackets=True)

    @staticmethod
    def from_dict(obj: Any) -> Optional["Node"]:
        return Node.from_string(obj)


class Relationship:
    def __init__(self, relation_name: str, relation_type: str, from_node: Node, to_node: Node,
                 properties: List[Property], where_condition: str, has_direction: bool):
        self.relation_name = relation_name
        self.relation_type = relation_type
        self.from_node = from_node
        self.to_node = to_node
        self.properties = properties
        self.where_condition = where_condition
        self.has_direction = has_direction

    @staticmethod
    def from_string(relation_description: str) -> Optional["Relationship"]:
        if relation_description is None:
            return None

        # we expect a node to be described in (node_name:Node_label)
        relation_directions = {
            "left-to-right": {"has_direction": True, "from_node": 0, "to_node": 1},
            "right-to-left": {"has_direction": True, "from_node": 1, "to_node": 0},
            "undefined": {"has_direction": False, "from_node": 0, "to_node": 1}
        }

        nodes = re.findall(r'\([^<>]*\)', relation_description)
        _relation_string = re.findall(r'\[[^<>]*]', relation_description)[0]
        _relation_string = re.sub(r"[\[\]]", "", _relation_string)
        _relation_components = _relation_string.split(":")
        _relation_name = _relation_components[0]
        _relation_type = _relation_components[1]

        if ">" in relation_description:
            direction = "left-to-right"
        elif "<" in relation_description:
            direction = "right-to-left"
        else:
            direction = "undefined"

        # TODO, implement properties and where condition

        _has_direction = relation_directions[direction]["has_direction"]
        _from_node = Node.from_string(nodes[relation_directions[direction]["from_node"]])
        _to_node = Node.from_string(nodes[relation_directions[direction]["to_node"]])

        return Relationship(relation_name=_relation_name, relation_type=_relation_type,
                            from_node=_from_node, to_node=_to_node, properties=[], where_condition="",
                            has_direction=_has_direction)

    @staticmethod
    def from_dict(obj: Any) -> Optional["Relationship"]:
        return Relationship.from_string(obj)

    def get_pattern(self, name: Optional[str] = None, exclude_nodes=True, with_brackets=False):
        rel_pattern_str = "$rel_name"
        if self.relation_type != "":
            rel_pattern_str = "$rel_name:$rel_type"

        name = name if name is not None else self.relation_name
        rel_pattern = Template(rel_pattern_str).substitute(rel_name=name,
                                                           rel_type=self.relation_type)

        if len(self.properties) > 0:
            properties_string = ",".join([prop.get_pattern() for prop in self.properties])
            rel_pattern_str = "$rel_pattern {$properties}"
            rel_pattern = Template(rel_pattern_str).substitute(rel_pattern=rel_pattern,
                                                               properties=properties_string)
        elif self.where_condition != "":
            rel_pattern_str = "$rel_pattern WHERE $where_condition"
            rel_pattern = Template(rel_pattern_str).substitute(rel_pattern=rel_pattern,
                                                               where_condition=self.where_condition)

        if exclude_nodes:
            if with_brackets:
                rel_pattern_str = "[$rel_pattern]"
                rel_pattern = Template(rel_pattern_str).substitute(rel_pattern=rel_pattern)
        else:
            from_node_pattern = self.from_node.get_pattern()
            to_node_pattern = self.to_node.get_pattern()
            rel_pattern_str = "($from_node) - [$rel_pattern] -> ($to_node)" if self.has_direction \
                else "($from_node) - [$rel_pattern] - ($to_node)"
            rel_pattern = Template(rel_pattern_str).substitute(from_node=from_node_pattern,
                                                               to_node=to_node_pattern,
                                                               rel_pattern=rel_pattern)

        return rel_pattern

    def __repr__(self):
        return self.get_pattern(exclude_nodes=False)


@dataclass
class RelationConstructorByNodes(ABC):
    from_node: Node
    to_node: Node
    prevalent_record: Union["Node", "Relationship"]

    @staticmethod
    def from_dict(obj: Any) -> Optional["RelationConstructorByNodes"]:
        if obj is None:
            return None

        _prevalent_record = RelationshipOrNode.from_string(obj.get("prevalent_record"))
        _from_node = Node.from_string(obj.get("from_node"))
        _to_node = Node.from_string(obj.get("to_node"))

        return RelationConstructorByNodes(prevalent_record=_prevalent_record, from_node=_from_node, to_node=_to_node)


class RelationshipOrNode(ABC):
    @staticmethod
    def from_string(relation_description: str) -> Union["Relationship", "Node"]:
        if relation_description is None:
            return None
        if "-" in relation_description:
            return Relationship.from_string(relation_description)
        else:
            return Node.from_string(relation_description)


@dataclass
class RelationConstructorByRelations(ABC):
    antecedents: List[Relationship]
    consequent: Relationship
    from_node: Node
    to_node: Node

    @staticmethod
    def from_dict(obj: Any) -> \
            Optional["RelationConstructorByRelations"]:
        if obj is None:
            return None

        _antecedents = [RelationshipOrNode.from_string(y) for y in obj.get("antecedents")]
        _consequent = Relationship.from_string(obj.get("consequent"))

        _from_node = _consequent.from_node
        _to_node = _consequent.to_node

        return RelationConstructorByRelations(antecedents=_antecedents, consequent=_consequent,
                                              from_node=_from_node, to_node=_to_node)

    def get_from_node_name(self):
        return self.consequent.from_node.name

    def get_to_node_name(self):
        return self.consequent.to_node.name

    def get_from_node_labels(self):
        return self.consequent.from_node.labels

    def get_to_node_labels(self):
        return self.consequent.to_node.labels

    def get_id_attribute_from_from_node(self):
        return get_id_attribute_from_label(self.from_node.labels[-1])

    def get_id_attribute_from_to_node(self):
        return get_id_attribute_from_label(self.to_node.labels[-1])

    def get_antecedent_query(self):
        if not all(isinstance(x, (Relationship, Node)) for x in self.antecedents):
            raise TypeError("Antecedents are not of type Relationship or Node")

        antecedents_query = [f"MATCH {antecedent.get_pattern()}" for antecedent in self.antecedents]
        antecedents_query = "\n".join(antecedents_query)

        return antecedents_query


@dataclass
class RelationConstructorByQuery(ABC):
    query: str

    @staticmethod
    def from_dict(obj: Any) -> Optional["RelationConstructorByQuery"]:
        if obj is None:
            return None

        _query = obj.get("query")

        return RelationConstructorByQuery(query=_query)


@dataclass
class NodesConstructorByQuery:
    query: str

    @staticmethod
    def from_dict(obj: Any) -> Optional["NodesConstructorByQuery"]:
        if obj is None:
            return None

        _query = obj.get("query")

        return NodesConstructorByQuery(query=_query)


class NodeConstructor:
    def __init__(self, prevalent_record: Optional[Union["Relationship", "Node"]],
                 node: Optional["Node"],
                 relation: Optional["Relationship"],
                 result: "Node",
                 set_properties: List[Property],
                 set_labels: str,
                 infer_observed: bool = False,
                 infer_corr_from_event_record: bool = False,
                 infer_corr_from_reified_parents: bool = False,
                 infer_reified_relation: bool = False):
        self.prevalent_record = prevalent_record
        self.relation = relation
        self.node = node
        self.result = result
        self.set_properties = set_properties
        self.set_labels = set_labels
        self.infer_prevalence_record = prevalent_record is not None
        self.infer_observed = infer_observed
        self.infer_corr_from_event_record = infer_corr_from_event_record
        self.infer_corr_from_reified_parents = infer_corr_from_reified_parents
        self.infer_reified_relation = infer_reified_relation

    @staticmethod
    def from_dict(obj: Any) -> "NodeConstructor":
        _prevalent_record = RelationshipOrNode.from_string(obj.get("prevalent_record"))
        _node = Relationship.from_string(obj.get("node"))
        _relation = Relationship.from_string(obj.get("relation"))
        _result = Node.from_string(obj.get("result"))
        _set_properties = obj.get("set_properties")
        if _set_properties is not None:
            _set_properties = _set_properties.split(",")
            _set_properties = [Property.from_string(prop) for prop in _set_properties]
        _set_labels = obj.get("set_labels")
        _infer_observed = replace_undefined_value(obj.get("infer_observed"), False)
        _infer_corr_from_event_record = replace_undefined_value(obj.get("infer_corr_from_event_record"), False)
        _infer_corr_from_reified_parents = replace_undefined_value(obj.get("infer_corr_from_reified_parents"), False)
        _infer_reified_relation = replace_undefined_value(obj.get("infer_reified_relation"), False)

        return NodeConstructor(prevalent_record=_prevalent_record,
                               relation=_relation,
                               node=_node,
                               result=_result,
                               infer_observed=_infer_observed,
                               infer_corr_from_event_record=_infer_corr_from_event_record,
                               infer_corr_from_reified_parents=_infer_corr_from_reified_parents,
                               infer_reified_relation=_infer_reified_relation,
                               set_properties=_set_properties,
                               set_labels=_set_labels)

    def __name__(self):
        if self.prevalent_record is not None:
            return "constructed_by_record"
        elif self.node is not None:
            return "constructed_by_node"
        elif self.relation is not None:
            return "constructed_by_relation"

    def get_label_string(self):
        return self.result.get_label_str()

    def get_labels(self, as_str=True):
        if as_str:
            return ",".join([f'"{label}"' for label in self.result.labels])
        return self.result.labels

    def get_prevalent_record_pattern(self, node_name: str):
        return self.prevalent_record.get_pattern(node_name)

    def get_keys(self):
        keys = []
        for prop in self.result.properties:
            key = prop.ref_attribute
            keys.append(key)
        return keys

    def get_where_condition(self, node_name: str = "record"):
        return " AND ".join(
            [f'''{node_name}.{key} IS NOT NULL AND {node_name}.{key} <> "Unknown"''' for key
             in self.get_keys()])

    def get_pattern(self, name: Optional[str] = None, with_brackets=False, with_properties=True):
        return self.result.get_pattern(name, with_brackets, with_properties)

    def __repr__(self):
        return self.result.get_pattern(with_brackets=True)

    def constructed_by_record(self):
        return self.prevalent_record is not None

    def constructed_by_node(self):
        return self.node is not None

    def constructed_by_relation(self):
        return self.relation is not None

    def get_set_result_properties_query(self):
        if self.set_properties is None:
            return None
        return ",".join([prop.get_pattern(is_set=False) for prop in self.set_properties])

    def get_idt_properties_query(self):
        if self.result.properties is None:
            return None
        return ",".join([prop.get_pattern(is_set=False) for prop in self.result.properties])

    def get_set_result_labels_query(self):
        if self.set_labels is None:
            return None
        return f"{self.result.get_name()}:{self.set_labels}"


class ConstructedNodes:
    def __init__(self, node_type: str, include: bool,
                 node_constructors: List["NodeConstructor"],
                 infer_df: bool,
                 include_label_in_df: bool,
                 merge_duplicate_df: bool,
                 delete_parallel_df: bool):
        self.node_type = node_type
        self.include = include
        self.node_constructors = node_constructors
        self.infer_df = infer_df
        self.include_label_in_df = include_label_in_df
        self.merge_duplicate_df = merge_duplicate_df
        self.delete_parallel_df = delete_parallel_df

        labels = self.node_constructors[0].get_labels()
        for constructor in self.node_constructors:
            if set(labels) != set(constructor.get_labels()):
                raise ValueError(f"The resulting nodes for {self.node_type} do not have the same labels")

    def __repr__(self):
        return f"(:{self.get_label_string()})"

    @staticmethod
    def from_dict(obj: Any) -> Optional["ConstructedNodes"]:
        if obj is None:
            return None
        _include = replace_undefined_value(obj.get("include"), True)
        if not _include:
            return None

        _type = obj.get("type")
        node_constructors = create_list(NodeConstructor, obj.get("constructor"))
        _infer_df = replace_undefined_value(obj.get("infer_df"), False)
        _include_label_in_df = _infer_df and replace_undefined_value(obj.get("include_label_in_df"), False)
        _merge_duplicate_df = _infer_df and replace_undefined_value(obj.get("merge_duplicate_df"), False)
        _delete_parallel_df = _infer_df and obj.get("delete_parallel_df")

        constructed_node = ConstructedNodes(include=_include, node_constructors=node_constructors,
                                            node_type=_type,
                                            infer_df=_infer_df,
                                            include_label_in_df=_include_label_in_df,
                                            merge_duplicate_df=_merge_duplicate_df,
                                            delete_parallel_df=_delete_parallel_df)

        # TODO check whether names match
        return constructed_node

    def get_label_string(self):
        return self.node_constructors[0].get_label_string()

    def get_labels(self):
        return self.node_constructors[0].get_labels()

    def get_df_label(self):
        if self.include_label_in_df:
            return f'DF_{self.node_type.upper()}'
        else:
            return f'DF'


class RelationConstructor:
    def __init__(self, prevalent_record: Optional[Union["Relationship", "Node"]],
                 nodes: List["Node"],
                 relations: List["Relationship"],
                 from_node: "Node",
                 to_node: "Node",
                 result: "Relationship",
                 set_properties: List[Property]):
        self.prevalent_record = prevalent_record
        self.from_node = from_node
        self.to_node = to_node
        self.relations = relations
        self.nodes = nodes
        self.result = result
        self.set_properties = set_properties

    @staticmethod
    def from_dict(obj: Any) -> "RelationConstructor":
        _prevalent_record = RelationshipOrNode.from_string(obj.get("prevalent_record"))
        _nodes = create_list(Node, obj.get("nodes"))
        _relations = create_list(Relationship, obj.get("relations"))
        _from_node = Node.from_string(obj.get("from_node"))
        _to_node = Node.from_string(obj.get("to_node"))
        _result = Relationship.from_string(obj.get("result"))
        _set_properties = obj.get("set_properties")
        if _set_properties is not None:
            _set_properties = _set_properties.split(",")
            _set_properties = [Property.from_string(prop) for prop in _set_properties]

        return RelationConstructor(prevalent_record=_prevalent_record,
                                   relations=_relations,
                                   nodes=_nodes,
                                   from_node=_from_node,
                                   to_node=_to_node,
                                   result=_result,
                                   set_properties=_set_properties, )

    def __name__(self):
        if self.prevalent_record is not None:
            return "constructed_by_record"
        elif self.nodes is not None:
            return "constructed_by_nodes"
        elif self.relations is not None:
            return "constructed_by_relations"

    def get_type(self):
        return self.result.relation_type

    def get_prevalent_record_pattern(self, node_name: str):
        return self.prevalent_record.get_pattern(node_name)

    def get_keys(self):
        keys = []
        for prop in self.result.properties:
            key = prop.ref_attribute
            keys.append(key)
        return keys

    def get_where_condition(self, node_name: str = "record"):
        return " AND ".join(
            [f'''{node_name}.{key} IS NOT NULL AND {node_name}.{key} <> "Unknown"''' for key
             in self.get_keys()])

    def get_pattern(self, name: Optional[str] = None, with_brackets=False, with_properties=True):
        return self.result.get_pattern(name, with_brackets, with_properties)

    def __repr__(self):
        return self.result.get_pattern(with_brackets=False, exclude_nodes=False)

    def constructed_by_record(self):
        return self.prevalent_record is not None

    def constructed_by_nodes(self):
        return len(self.nodes) > 0

    def constructed_by_relations(self):
        return len(self.relations) > 0

    def get_set_result_properties_query(self):
        if self.set_properties is None:
            return None
        return ",".join([prop.get_pattern(is_set=True) for prop in self.set_properties])


    def get_relations_query(self):
        relation_queries = [f"MATCH {relation.get_pattern(exclude_nodes=False, with_brackets=True)}" for relation in self.relations]
        relation_query = "\n".join(relation_queries)
        return relation_query

@dataclass
class ConstructedRelation:
    include: bool
    type: str
    relation_constructors: List["RelationConstructor"]

    @staticmethod
    def from_dict(obj: Any) -> Optional["ConstructedRelation"]:
        if obj is None:
            return None
        _include = replace_undefined_value(obj.get("include"), True)
        if not _include:
            return None

        _type = obj.get("type")
        _relation_constructors = create_list(RelationConstructor, obj.get("constructor"))
        return ConstructedRelation(_include, _type, relation_constructors=_relation_constructors)

    def __repr__(self):
        return f"[:{self.type}]"


class SemanticHeader:
    def __init__(self, name: str, version: str,
                 nodes: List[ConstructedNodes], relations: List[ConstructedRelation]):
        self.name = name
        self.version = version
        self.nodes = nodes
        self.relations = relations

    def get_entity(self, entity_type) -> Optional[ConstructedNodes]:
        for entity in self.nodes:
            if entity_type == entity.node_type:
                return entity
        return None

    @staticmethod
    def from_dict(obj: Any) -> Optional["SemanticHeader"]:
        if obj is None:
            return None
        _name = obj.get("name")
        _version = obj.get("version")
        _nodes = create_list(ConstructedNodes, obj.get("nodes"))
        _relations = create_list(ConstructedRelation, obj.get("relations"))
        return SemanticHeader(_name, _version, _nodes, _relations)

    @staticmethod
    def create_semantic_header(path: Path):
        with open(path) as f:
            json_semantic_header = json.load(f)

        semantic_header = SemanticHeader.from_dict(json_semantic_header)
        return semantic_header

    def get_node_by_record_constructors(self, node_types: Optional[List[str]]) -> List[NodeConstructor]:
        node_constructors = []
        for node in self.nodes:
            if node_types is None or node.node_type in node_types:
                node_constructors.extend([
                    node_constructor for node_constructor in node.node_constructors if
                    node_constructor.constructed_by_record()
                ])
        return node_constructors

    def get_nodes_constructed_by_relations(self, node_types: Optional[List[str]] = None,
                                           only_include_delete_parallel_df=False) -> Dict[
        str, List[NodeConstructor]]:
        node_constructors = {}
        for node in self.nodes:
            if only_include_delete_parallel_df and not node.delete_parallel_df:
                continue
            if node_types is None or node.node_type in node_types:
                for node_constructor in node.node_constructors:
                    if node_constructor.constructed_by_relation():
                        if node.node_type not in node_constructors:
                            node_constructors[node.node_type] = []
                        node_constructors[node.node_type].append(node_constructor)
        return node_constructors

    def get_entities_constructed_by_query(self):
        return [entity for entity in self.nodes if
                entity.constructor_type == "EntityConstructorByQuery"]

    def get_relations_constructed_by_nodes(self, rel_types: Optional[List[str]]):
        return self._get_relations_constructed_by_specific_constructor(rel_types, constructor="nodes")

    def get_relations_constructed_by_relations(self, rel_types: Optional[List[str]]):
        return self._get_relations_constructed_by_specific_constructor(rel_types, constructor="relations")

    def get_relations_constructed_by_record(self, rel_types: Optional[List[str]]):
        return self._get_relations_constructed_by_specific_constructor(rel_types, constructor="record")

    def _get_relations_constructed_by_specific_constructor(self, rel_types, constructor):
        rel_constructors = []
        for relation in self.relations:
            if rel_types is None or relation.type in rel_types:
                if constructor == "nodes":
                    rel_constructors.extend(
                        [rel_constructor for rel_constructor in relation.relation_constructors if
                         rel_constructor.constructed_by_nodes()])
                elif constructor == "relations":
                    rel_constructors.extend(
                        [rel_constructor for rel_constructor in relation.relation_constructors if
                         rel_constructor.constructed_by_relations()])
                elif constructor == "record":
                    rel_constructors.extend(
                        [rel_constructor for rel_constructor in relation.relation_constructors if
                         rel_constructor.constructed_by_record()])
        return rel_constructors

    def get_relations_derived_from_relations(self):
        return [relation for relation in self.relations if
                "RelationConstructorByRelations" in relation.constructor_type]

    def get_relations_derived_from_query(self):
        return [relation for relation in self.relations if
                relation.constructor_type == "RelationConstructorByQuery"]
