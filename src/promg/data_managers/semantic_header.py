import json
import warnings
from abc import ABC
from pathlib import Path
from string import Template
from typing import List, Any, Optional, Union, Dict

from dataclasses import dataclass

from .properties import Properties
from ..utilities.auxiliary_functions import replace_undefined_value, create_list, get_id_attribute_from_label
from ..utilities.configuration import Configuration
import re

from ..utilities.singleton import Singleton


class Node:
    def __init__(self, name: str, labels: List[str], properties: Properties, where_condition: str):
        self.name = name
        self.labels = labels
        self.properties = properties
        self.where_condition = where_condition

    @classmethod
    def from_string(cls, node_description: str) -> Optional["Node"]:
        if node_description is None:
            return None
        # we expect a node to be described in (node_name:Node_label)
        node_description = re.sub(r"[()]", "", node_description)
        node_components = node_description.split(":", 1)
        name = node_components[0]
        labels = ""
        where_condition = ""
        properties = None
        if len(node_components) > 1:
            node_labels_prop_where = node_components[1]
            node_labels_prop_where = node_labels_prop_where.replace("'", "\"")
            if "WHERE" in node_labels_prop_where:
                labels = node_labels_prop_where.split(" WHERE ")[0]
                where_condition = node_labels_prop_where.split(" WHERE ")[1]
            elif "{" in node_labels_prop_where:
                labels = node_labels_prop_where.split(" {")[0]
                property_str = node_labels_prop_where.split(" {")[1]
                properties = Properties.from_string(property_str)
            else:
                labels = node_labels_prop_where

        labels = labels.strip()
        labels = labels.split(":")

        return cls(name=name,
                   labels=labels,
                   properties=properties,
                   where_condition=where_condition)

    def get_identifier(self):
        return self.properties.get_identifier_property_attributes()

    def get_name(self, with_brackets=False):
        if with_brackets:
            return f"({self.name})"
        else:
            return self.name

    def get_condition_string(self, with_brackets=False, with_where=False, with_optional=False):
        if self.properties is not None and self.properties.has_required_properties(with_optional):
            return self.properties.get_string(with_brackets, with_optional)
        elif self.where_condition != "":
            return self._get_where_condition_string(with_where)
        else:
            return ""

    def _get_where_condition_string(self, with_where=False):
        condition = self.where_condition
        if with_where:
            condition_string = "WHERE $where_condition"
            condition = Template(condition_string).substitute(where_condition=condition)
        return condition

    def get_pattern(self, name: Optional[str] = None, with_brackets=False, with_properties=True, forbidden_label=None):
        node_pattern_str = "$node_name"
        sep = ":"
        if self.get_label_str() != "":
            node_pattern_str = "$node_name:$node_label"
            if forbidden_label is not None:
                sep = "&"
                node_pattern_str += "&!$forbidden_label"

        if name is None:
            node_pattern = Template(node_pattern_str).substitute(node_name=self.name,
                                                                 node_label=self.get_label_str(sep=sep),
                                                                 forbidden_label=forbidden_label)
        else:
            node_pattern = Template(node_pattern_str).substitute(node_name=name,
                                                                 node_label=self.get_label_str(sep=sep),
                                                                 forbidden_label=forbidden_label)
        if with_properties:
            node_pattern_str = "$node_pattern $condition_string"
            node_pattern = Template(node_pattern_str).substitute(node_pattern=node_pattern,
                                                                 condition_string=self.get_condition_string(
                                                                     with_brackets=True, with_where=True))
        if with_brackets:
            node_pattern_str = "($node_pattern)"
            node_pattern = Template(node_pattern_str).substitute(node_pattern=node_pattern)

        return node_pattern

    def get_label_str(self, include_first_colon=False, as_list=False, sep=":"):
        if as_list:
            str = ",".join([f'"{label}"' for label in self.labels])
            return f'[{str}]'

        if len(self.labels) > 0:
            return sep * include_first_colon + sep.join(self.labels)
        return ""

    def get_set_optional_properties_query(self, node_name):
        if self.properties is None:
            return None
        return self.properties.get_set_optional_properties_query(name=node_name)

    def get_idt_properties_query(self, node_name):
        return self.properties.get_idt_properties_query(node_name=node_name)

    def __repr__(self):
        return self.get_pattern(with_brackets=True)

    @staticmethod
    def from_dict(obj: Any) -> Optional["Node"]:
        return Node.from_string(obj)


class RecordNode(Node):
    def __init__(self, name: str, labels: List[str], properties: Properties, where_condition: str):
        node_labels = []
        record_types = []
        for label in labels:
            if "Record" in label and label != "Record":
                record_types.append(label)
            else:
                node_labels.append(label)

        if "Record" not in node_labels:
            node_labels.append("Record")

        super().__init__(name, node_labels, properties, where_condition)
        self.record_types = record_types

    def get_record_type_match(self, name, forbidden_label=None):
        all_matches = ""
        for record_type in self.record_types:
            if record_type != forbidden_label:
                match_str = '''MATCH ($record_name:Record) - [:IS_OF_TYPE] -> (:RecordType {type:"$record_type"}) \n'''
            match = Template(match_str).substitute(record_name=name if name is not None else self.name,
                                                   record_type=record_type)
            all_matches += match
        return all_matches


class Relationship:
    def __init__(self, relation_name: str, relation_types: List[str], from_node: Node, to_node: Node,
                 properties: Properties, where_condition: str, has_direction: bool):
        self.relation_name = relation_name
        self.relation_types = relation_types
        self.from_node = from_node
        self.to_node = to_node
        self.properties = properties
        self.where_condition = where_condition
        self.has_direction = has_direction

    def get_relation_type(self):
        return self.relation_types[0]

    def get_relation_types_str(self, include_first_colon=False, as_list=False):
        if as_list:
            str = ",".join([f'"{label}"' for label in self.relation_types])
            return f'[{str}]'
        if len(self.relation_types) > 0:
            return ":" * include_first_colon + ":".join(self.relation_types)
        return ""

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

        if "{" in _relation_string:  # properties are defined
            name_and_type = _relation_string.split(" {")[0]
            property_str = _relation_string.split(" {")[1]
            properties = Properties.from_string(property_str)
        else:
            name_and_type = _relation_string
            properties = None

        _relation_components = name_and_type.split(":", maxsplit=1)
        _relation_name = _relation_components[0]
        _relation_types = _relation_components[1]
        _relation_types = _relation_types.split(":")

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

        return Relationship(relation_name=_relation_name, relation_types=_relation_types,
                            from_node=_from_node, to_node=_to_node, properties=properties, where_condition="",
                            has_direction=_has_direction)

    @staticmethod
    def from_dict(obj: Any) -> Optional["Relationship"]:
        return Relationship.from_string(obj)

    def get_pattern(self, name: Optional[str] = None, exclude_nodes=True, with_properties=True, with_brackets=False):
        # First, make pattern consisting of rel_name:rel_type (if defined)
        rel_pattern_str = "$rel_name"
        if self.get_relation_type() != "":
            rel_pattern_str = "$rel_name:$rel_type"

        name = name if name is not None else self.relation_name
        rel_pattern = Template(rel_pattern_str).substitute(rel_name=name,
                                                           rel_type=self.get_relation_types_str())

        # add properties if requested and there are properties defined
        if with_properties and self.properties is not None:
            properties_string = self.properties.get_string(with_brackets=False, with_optional=False)
            rel_pattern_str = "$rel_pattern {$properties}"
            rel_pattern = Template(rel_pattern_str).substitute(rel_pattern=rel_pattern,
                                                               properties=properties_string)
        # add where condition if requested and where condition is defined
        elif with_properties and self.where_condition != "":
            rel_pattern_str = "$rel_pattern WHERE $where_condition"
            rel_pattern = Template(rel_pattern_str).substitute(rel_pattern=rel_pattern,
                                                               where_condition=self.where_condition)
        # don't add from and to nodes if they should be excluded
        if exclude_nodes:
            if with_brackets:  # add brackets
                rel_pattern_str = "[$rel_pattern]"
                rel_pattern = Template(rel_pattern_str).substitute(rel_pattern=rel_pattern)
        else:  # add from and to nodes (brackets are always added)
            from_node_pattern = self.from_node.get_pattern()
            to_node_pattern = self.to_node.get_pattern()
            rel_pattern_str = "($from_node) - [$rel_pattern] -> ($to_node)" if self.has_direction \
                else "($from_node) - [$rel_pattern] - ($to_node)"
            rel_pattern = Template(rel_pattern_str).substitute(from_node=from_node_pattern,
                                                               to_node=to_node_pattern,
                                                               rel_pattern=rel_pattern)

        return rel_pattern

    def get_set_optional_properties_query(self, relation_name):
        if self.properties is not None:
            return self.properties.get_set_optional_properties_query(name=relation_name)
        return None

    def __repr__(self):
        return self.get_pattern(exclude_nodes=False)


@dataclass
class RelationConstructorByNodes(ABC):
    from_node: Node
    to_node: Node
    prevalent_record: Union["RecordNode", "Node", "Relationship"]

    @staticmethod
    def from_dict(obj: Any) -> Optional["RelationConstructorByNodes"]:
        if obj is None:
            return None

        _prevalent_record = RelationshipOrNode.from_string(obj.get("prevalent_record"), is_record=True)
        _from_node = Node.from_string(obj.get("from_node"))
        _to_node = Node.from_string(obj.get("to_node"))

        return RelationConstructorByNodes(prevalent_record=_prevalent_record, from_node=_from_node, to_node=_to_node)


class RelationshipOrNode(ABC):
    @staticmethod
    def from_string(description: str, is_record=False) -> Union["Relationship", "Node", "RecordNode"]:
        if description is None:
            return None
        if "-" in description:
            return Relationship.from_string(description)
        else:
            if is_record:
                return RecordNode.from_string(description)
            else:
                return Node.from_string(description)


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


class InferredRelationship:
    def __init__(self, record_types: List[str] = None, relation_type: str = "CORR", event: Node = None):
        self.record_types = record_types if record_types is not None else ["EventRecord"]
        self.relation_type = relation_type
        if event is None:
            event = Node.from_string("(event:Event)")
        self.event = event

    @staticmethod
    def from_dict(obj):
        if obj is None:
            return None

        _event = Node.from_string(obj.get("event"))
        _record_labels = obj.get("record_labels").split(":")
        _relation_type = obj.get("relation_type")

        return InferredRelationship(event=_event, record_types=_record_labels, relation_type=_relation_type)

    def get_record_type_match(self, record_name="record"):
        all_matches = ""
        for record_type in self.record_types:
            match_str = '''MATCH ($record_name:Record) - [:IS_OF_TYPE] -> (:RecordType {type:"$record_type"}) \n'''
            match = Template(match_str).substitute(record_name=record_name,
                                                   record_type=record_type)
            all_matches += match
        return all_matches


class NodeConstructor:
    def __init__(self, prevalent_record: Optional[Union["Relationship", "RecordNode"]],
                 node: Optional["Node"],
                 relation: Optional["Relationship"],
                 use_inference: bool,
                 result: "Node",
                 set_labels: str,
                 infer_observed: bool = False,
                 infer_observed_str: str = "OBSERVED",
                 infer_corr_from_event_record: bool = False,
                 infer_corr_from_entity_record: bool = False,
                 infer_corr_from_reified_parents: bool = False,
                 inferred_relationships: List[InferredRelationship] = None,
                 event_label: str = "Event",
                 corr_type: str = "CORR",
                 infer_reified_relation: bool = False,
                 merge: bool = None):
        # node can be constructed using several methods
        # 1) via a prevalent record
        self.prevalent_record = prevalent_record
        # 2) via a relation
        self.relation = relation
        # 3) via a node
        self.node = node
        # 4) via inference, custom code provided by user
        self.use_inference = use_inference
        self.result = result
        self.set_labels = set_labels
        self.infer_prevalence_record = prevalent_record is not None
        self.infer_observed = infer_observed
        self.infer_observed_str = infer_observed_str
        self.infer_corr_from_event_record = infer_corr_from_event_record
        self.infer_corr_from_entity_record = infer_corr_from_entity_record
        self.infer_corr_from_reified_parents = infer_corr_from_reified_parents
        self.inferred_relationships = inferred_relationships
        self.event_label = event_label
        self.corr_type = corr_type
        self.infer_reified_relation = infer_reified_relation
        self.merge = merge
        if self.merge is None: # if merge is undefined, we try to determine it using common labels
            self.merge = self.determine_merge()

    @staticmethod
    def from_dict(obj: Any) -> "NodeConstructor":
        _prevalent_record = RelationshipOrNode.from_string(obj.get("prevalent_record"), is_record=True)
        _node = Relationship.from_string(obj.get("node"))
        _relation = Relationship.from_string(obj.get("relation"))
        _use_inference = replace_undefined_value(obj.get("use_inference"), False)
        _result = Node.from_string(obj.get("result"))
        _set_labels = obj.get("set_labels")
        _infer_observed = replace_undefined_value(obj.get("infer_observed"), False)
        _infer_observed_str = replace_undefined_value(obj.get("infer_observed_str"), "OBSERVED")
        _infer_corr_from_event_record = replace_undefined_value(obj.get("infer_corr_from_event_record"), False)
        _infer_corr_from_entity_record = replace_undefined_value(obj.get("infer_corr_from_entity_record"), False)
        _infer_corr_from_reified_parents = replace_undefined_value(obj.get("infer_corr_from_reified_parents"), False)
        _corr_type = replace_undefined_value(obj.get("corr_type"), "CORR")
        _event_label = replace_undefined_value(obj.get("event_label"), "Event")
        _infer_reified_relation = replace_undefined_value(obj.get("infer_reified_relation"), False)

        _inferred_relations = create_list(InferredRelationship, obj.get("inferred_relationships"))

        _merge = obj.get("merge")

        return NodeConstructor(prevalent_record=_prevalent_record,
                               relation=_relation,
                               node=_node,
                               use_inference=_use_inference,
                               result=_result,
                               infer_observed=_infer_observed,
                               infer_observed_str=_infer_observed_str,
                               infer_corr_from_event_record=_infer_corr_from_event_record,
                               infer_corr_from_entity_record=_infer_corr_from_entity_record,
                               infer_corr_from_reified_parents=_infer_corr_from_reified_parents,
                               inferred_relationships=_inferred_relations,
                               corr_type=_corr_type,
                               event_label=_event_label,
                               infer_reified_relation=_infer_reified_relation,
                               set_labels=_set_labels,
                               merge=_merge)

    def __name__(self):
        if self.prevalent_record is not None:
            return "constructed_by_record"
        elif self.node is not None:
            return "constructed_by_node"
        elif self.relation is not None:
            return "constructed_by_relation"
        elif self.use_inference:
            return "constructed_by_inference"

    def determine_merge(self):
        return ("Event" not in self.get_labels() and "EntityAttribute" not in self.get_labels()) \
            or ("EventType" in self.get_labels())

    def get_merge(self):
        return self.merge

    def get_label_string(self):
        return self.result.get_label_str()

    def get_labels(self, as_str=True):
        if as_str:
            return ",".join([f'"{label}"' for label in self.result.labels])
        return self.result.labels

    def get_prevalent_record_pattern(self, node_name: str = "record", forbidden_label: str = None):
        return self.prevalent_record.get_pattern(name=node_name, forbidden_label=forbidden_label)

    def get_record_types(self):
        return ":" + ":".join(self.prevalent_record.record_types)

    def get_prevalent_match_record_pattern(self, node_name: str = "record", forbidden_label: str = None):
        return self.prevalent_record.get_record_type_match(name=node_name, forbidden_label=forbidden_label)

    def get_pattern(self, name: Optional[str] = None, with_brackets=False, with_properties=True):
        return self.result.get_pattern(name, with_brackets=with_brackets, with_properties=with_properties)

    def __repr__(self):
        return self.result.get_pattern(with_brackets=True)

    def constructed_by_record(self):
        return self.prevalent_record is not None

    def constructed_by_node(self):
        return self.node is not None

    def constructed_by_relation(self):
        return self.relation is not None

    def get_set_result_properties_query(self, node_name=None):
        if node_name is None:
            node_name = self.result.name
        set_optional_properties_str = self.result.get_set_optional_properties_query(node_name=node_name)
        if set_optional_properties_str is not None:
            return f"SET {set_optional_properties_str}"
        else:
            return ""

    def get_idt_properties_query(self, node_name="n"):
        if node_name is None:
            node_name = self.result.name
        return self.result.get_idt_properties_query(node_name=node_name)

    def get_set_result_labels_query(self):
        if self.set_labels is None:
            return None
        return f"{self.result.get_name()}:{self.set_labels}"


class ConstructedNodes:
    def __init__(self, node_type: str, include: bool,
                 node_constructors: List["NodeConstructor"],
                 infer_df: bool,
                 include_label_in_df: bool,
                 merge_duplicate_df: bool):
        self.type = node_type
        self.include = include
        self.node_constructors = node_constructors
        self.infer_df = infer_df
        self.include_label_in_df = include_label_in_df
        self.merge_duplicate_df = merge_duplicate_df

    def __repr__(self):
        return f"(:{self.get_label_string()})"

    def get_identifier_properties(self):
        identifiers = None
        for node in self.node_constructors:
            new_identifers = node.result.get_identifier()
            if identifiers is None:
                identifiers = new_identifers
            else:
                if set(new_identifers) != set(identifiers):
                    raise ValueError(
                        f"Identifiers for {self.type} are different across constructors {identifiers} != "
                        f"{new_identifers}")
        return identifiers

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

        constructed_node = ConstructedNodes(include=_include, node_constructors=node_constructors,
                                            node_type=_type,
                                            infer_df=_infer_df,
                                            include_label_in_df=_include_label_in_df,
                                            merge_duplicate_df=_merge_duplicate_df)

        # TODO check whether names match
        return constructed_node

    def get_label_string(self):
        if len(self.node_constructors) == 0:
            return self.type
        return self.node_constructors[0].get_label_string()

    def get_labels(self, as_str=True):
        if len(self.node_constructors) == 0:
            return self.type
        return self.node_constructors[0].get_labels(as_str=as_str)

    def get_corr_type_strings(self):
        corr_types = list(set([node_constructor.corr_type for node_constructor in self.node_constructors]))
        return "|".join(corr_types)

    def get_df_label(self):
        return self._get_df_label_affix(include_label=self.include_label_in_df, affix="")

    def get_df_a_label(self, include_label_in_df_a: bool = None):
        include_label_in_df_a = self.include_label_in_df if include_label_in_df_a is None else include_label_in_df_a
        return self._get_df_label_affix(include_label=include_label_in_df_a, affix="A")

    def get_df_ti_label(self):
        return self._get_df_label_affix(include_label=self.include_label_in_df, affix="TI")

    def _get_df_label_affix(self, include_label, affix=""):
        df = "DF" if affix == "" else f"DF_{affix}"
        df = f'{df}_{self.type.upper()}' if include_label else df
        return df


class RelationConstructor:
    def __init__(self, prevalent_record: Optional[Union["Relationship", "RecordNode"]],
                 nodes: List["Node"],
                 relations: List["Relationship"],
                 use_inference: bool,
                 from_node: "Node",
                 to_node: "Node",
                 result: "Relationship",
                 model_as_node: bool,
                 infer_corr_from_reified_parents: bool,
                 corr_type: str):
        # relations can be constructed using several methods
        # 1) via a prevalent record
        self.prevalent_record = prevalent_record
        # 2) via another relations
        self.relations = relations
        # 3) via other nodes
        self.nodes = nodes
        # 4) via inference, custom code provided by user
        self.use_inference = use_inference

        self.from_node = from_node
        self.to_node = to_node
        self.result = result
        self.model_as_node = model_as_node
        self.infer_corr_from_reified_parents = infer_corr_from_reified_parents
        self.corr_type = corr_type

    @staticmethod
    def from_dict(obj: Any, model_as_node: bool) -> "RelationConstructor":
        _prevalent_record = RelationshipOrNode.from_string(obj.get("prevalent_record"), is_record=True)
        _nodes = create_list(Node, obj.get("nodes"))
        _relations = create_list(Relationship, obj.get("relations"))
        _use_inference = replace_undefined_value(obj.get("use_inference"), False)

        _from_node = Node.from_string(obj.get("from_node"))
        _to_node = Node.from_string(obj.get("to_node"))
        _result = Relationship.from_string(obj.get("result"))

        _infer_corr_from_reified_parents = replace_undefined_value(obj.get("infer_corr_from_reified_parents"), False)
        _corr_type = replace_undefined_value(obj.get("corr_type"), "CORR")

        return RelationConstructor(prevalent_record=_prevalent_record,
                                   relations=_relations,
                                   nodes=_nodes,
                                   use_inference=_use_inference,
                                   from_node=_from_node,
                                   to_node=_to_node,
                                   result=_result,
                                   model_as_node=model_as_node,
                                   infer_corr_from_reified_parents=_infer_corr_from_reified_parents,
                                   corr_type=_corr_type)

    def __name__(self):
        if self.prevalent_record is not None:
            return "constructed_by_record"
        elif self.nodes is not None:
            return "constructed_by_nodes"
        elif self.relations is not None:
            return "constructed_by_relations"
        elif self.use_inference:
            return "constructed_by_inference"

    def get_type(self):
        return self.result.get_relation_type()

    def get_prevalent_record_pattern(self, node_name: str):
        return self.prevalent_record.get_pattern(node_name)

    def get_prevalent_match_record_pattern(self, node_name: str = "record"):
        return self.prevalent_record.get_record_type_match(name=node_name)

    def get_pattern(self, name: Optional[str] = None, with_brackets=False, with_properties=True, exclude_nodes=False):
        return self.result.get_pattern(name=name, with_brackets=with_brackets, with_properties=with_properties,
                                       exclude_nodes=exclude_nodes)

    def __repr__(self):
        return self.result.get_pattern(with_brackets=False, exclude_nodes=False)

    def constructed_by_record(self, modeled_as_nodes):
        # A = self.prevalent_record is not None
        # B = modeled_as_node -> self.model_as_node == not modeled_as_node OR self.model_as_node
        # C = A AND B == len(self.relations) > 0 AND (not modeled_as_node OR self.model_as_node)
        return (self.prevalent_record is not None) and (not modeled_as_nodes or self.model_as_node)

    def constructed_by_nodes(self, modeled_as_nodes):
        # A = len(self.nodes) > 0
        # B = modeled_as_node -> self.model_as_node == not modeled_as_node OR self.model_as_node
        # C = A AND B == len(self.relations) > 0 AND (not modeled_as_node OR self.model_as_node)
        return (len(self.nodes) > 0) and (not modeled_as_nodes or self.model_as_node)

    def constructed_by_relations(self, modeled_as_nodes):
        # A = len(self.relations) > 0
        # B = modeled_as_node -> self.model_as_node == not modeled_as_node OR self.model_as_node
        # C = A AND B == len(self.relations) > 0 AND (not modeled_as_node OR self.model_as_node)
        return (len(self.relations) > 0) and (not modeled_as_nodes or self.model_as_node)

    def constructed_by_inference(self, modeled_as_nodes):
        # A = self.use_inference
        # B = modeled_as_node -> self.model_as_node == not modeled_as_node OR self.model_as_node
        # C = A AND B == self.use_inference AND (not modeled_as_node OR self.model_as_node)
        return self.use_inference and (not modeled_as_nodes or self.model_as_node)

    def get_set_result_properties_query(self, relation_name=None):
        if relation_name is None:
            relation_name = self.result.relation_name
        set_optional_properties_str = self.result.get_set_optional_properties_query(relation_name=relation_name)
        if set_optional_properties_str is not None:
            return f"SET {set_optional_properties_str}"
        else:
            return ""

    def get_relations_query(self):
        relation_queries = [f"MATCH {relation.get_pattern(exclude_nodes=False, with_brackets=True)}" for relation in
                            self.relations]
        relation_query = "\n".join(relation_queries)
        return relation_query

    def get_node_query(self):
        node_queries = [f"MATCH {node.get_pattern(with_brackets=True)}" for node in self.nodes]
        node_query = "\n".join(node_queries)
        return node_query

    def get_type_string(self):
        return self.result.get_relation_types_str()

    def get_types(self):
        return self.result.relation_types


@dataclass
class ConstructedRelation:
    include: bool
    type: str
    relation_constructors: List["RelationConstructor"]
    model_as_node: bool
    infer_df: bool
    include_label_in_df: bool
    merge_duplicate_df: bool

    @staticmethod
    def from_dict(obj: Any) -> Optional["ConstructedRelation"]:
        if obj is None:
            return None
        _include = replace_undefined_value(obj.get("include"), True)
        if not _include:
            return None

        _type = obj.get("type")
        _model_as_node = replace_undefined_value(obj.get("model_as_node"), False)
        _relation_constructors = create_list(RelationConstructor, obj.get("constructor"), _model_as_node)
        _infer_df = replace_undefined_value(obj.get("infer_df"), False)
        _include_label_in_df = _infer_df and replace_undefined_value(obj.get("include_label_in_df"), False)
        _merge_duplicate_df = _infer_df and replace_undefined_value(obj.get("merge_duplicate_df"), False)

        if not _model_as_node and _infer_df:
            warnings.warn("Cannot infer df for relations that are modeled as edges")
        return ConstructedRelation(_include, _type,
                                   relation_constructors=_relation_constructors,
                                   model_as_node=_model_as_node,
                                   infer_df=_infer_df,
                                   include_label_in_df=_include_label_in_df,
                                   merge_duplicate_df=_merge_duplicate_df
                                   )

    def __repr__(self):
        return f"[:{self.type}]"

    def get_label_string(self):
        if self.model_as_node:
            if len(self.relation_constructors) == 0:
                return self.type
            return self.relation_constructors[0].get_type_string()
        else:
            raise ValueError("Relationship is not modelled as node, hence does not have a label string."
                             "Set model_as_node = true in Semantic Header")

    def get_labels(self):
        if len(self.relation_constructors) == 0:
            return self.type
        return self.relation_constructors[0].get_types()

    def get_corr_type_strings(self):
        corr_types = list(set([node_constructor.corr_type for node_constructor in self.relation_constructors]))
        return "|".join(corr_types)

    def get_df_label(self):
        return self._get_df_label_affix(include_label=self.include_label_in_df, affix="")

    def get_df_a_label(self, include_label_in_df_a: bool = None):
        include_label_in_df_a = self.include_label_in_df if include_label_in_df_a is None else include_label_in_df_a
        return self._get_df_label_affix(include_label=include_label_in_df_a, affix="A")

    def get_df_ti_label(self):
        return self._get_df_label_affix(include_label=self.include_label_in_df, affix="TI")

    def _get_df_label_affix(self, include_label, affix=""):
        df = "DF" if affix == "" else f"DF_{affix}"
        df = f'{df}_{self.type.upper()}' if include_label else df
        return df


class RecordConstructor:
    def __init__(self, record_labels: List[str],
                 required_attributes: List[str], optional_attributes: List[str], node_name: str = "record",
                 prevalent_record: Optional["RecordNode"] = Node.from_string("(record:Record)")):
        self.node_name = node_name
        self.prevalent_record = prevalent_record
        self.record_labels = record_labels
        self.required_attributes = required_attributes
        self.optional_attributes = optional_attributes

    @staticmethod
    def from_dict(obj: Any) -> "RecordConstructor":
        if isinstance(obj, str):
            return RecordConstructor.from_str(obj)
        _prevalent_record = RecordNode.from_string(obj.get("prevalent_record"))
        _record_labels = obj.get("record_labels").split(":")
        _required_attributes = obj.get("required_attributes")
        _optional_attributes = obj.get("optional_attributes")

        return RecordConstructor(prevalent_record=_prevalent_record, record_labels=_record_labels,
                                 required_attributes=_required_attributes,
                                 optional_attributes=_optional_attributes)

    @staticmethod
    def from_str(obj: str) -> "RecordConstructor":
        obj = re.sub(r"[\(\)]", "", obj)
        _node_name = obj.split(":", maxsplit=1)[0].strip()
        if _node_name == "":
            _node_name = "record"
        record_description = obj.split(":", maxsplit=1)[1].strip()
        if "WHERE" in record_description.upper():
            labels = record_description.split("WHERE")[0].replace(" ", "")
            condition_and_properties = record_description.split("WHERE")[1].strip()
            condition = condition_and_properties.split("{")[0].strip()
            properties = condition_and_properties.split("{")[1].replace("}", "").strip()
        else:
            labels = record_description.split("{")[0].replace(" ", "")
            condition = None
            properties = record_description.split("{")[1].replace("}", "").strip()

        _record_labels = labels.split(":")
        prevalent_record_str = f"({_node_name}:Record)"
        if condition is not None:
            prevalent_record_str = f"({_node_name}:Record WHERE {condition})"
        _prevalent_record = Node.from_string(prevalent_record_str)
        properties = properties.split(",")
        _required_attributes = [prop.strip() for prop in properties if "OPTIONAL" not in prop.upper()]
        _optional_attributes = [prop.replace("OPTIONAL", "").strip() for prop in properties if
                                "OPTIONAL" in prop.upper()]

        return RecordConstructor(node_name=_node_name, record_labels=_record_labels, prevalent_record=_prevalent_record,
                                 required_attributes=_required_attributes, optional_attributes=_optional_attributes)

    def get_prevalent_record_pattern(self, record_name: str = "record"):
        return self.prevalent_record.get_pattern(record_name)

    def get_prevalent_match_record_pattern(self, record_name: str = "record"):
        return self.prevalent_record.get_record_type_match(name=record_name)

    def get_additional_conditions(self, record_name: str = "record"):
        cond = self.prevalent_record.get_condition_string(with_brackets=False, with_where=False)
        if cond != "":
            return f"AND {self.prevalent_record.get_condition_string(with_brackets=False, with_where=False)}"
        return ""

    def get_required_attributes_is_not_null_pattern(self, record_name: str = "record"):
        return " AND ".join(
            [f'''{record_name}.{attribute} IS NOT NULL''' for attribute in self.required_attributes])

    def get_record_labels_pattern(self):
        return ":".join(self.record_labels)

    def get_label_list(self, as_str=True):
        if as_str:
            return "[" + ",".join([f'"{label}"' for label in self.record_labels]) + "]"
        return self.record_labels


class SemanticHeader:
    def __init__(self, name: str, version: str,
                 records: List["RecordConstructor"],
                 nodes: List[ConstructedNodes],
                 relations: List[ConstructedRelation]):
        self.name = name
        self.version = version
        self.records = records
        self.nodes = nodes
        self.relations = relations

    def get_entity(self, entity_type) -> Optional[ConstructedNodes]:
        for entity in self.nodes + self.relations:
            if entity_type == entity.type:
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
        _records = create_list(RecordConstructor, obj.get("records"))
        return SemanticHeader(name=_name, version=_version, records=_records, nodes=_nodes, relations=_relations)

    @staticmethod
    def create_semantic_header(config: Configuration):
        path = config.semantic_header_path

        with open(path) as f:
            json_semantic_header = json.load(f)

        semantic_header = SemanticHeader.from_dict(json_semantic_header)
        return semantic_header

    def get_node_by_record_constructors(self, node_types: Optional[List[str]]) -> List[NodeConstructor]:
        node_constructors = []
        for node in self.nodes:
            if node_types is None or node.type in node_types:
                node_constructors.extend([
                    node_constructor for node_constructor in node.node_constructors if
                    node_constructor.constructed_by_record()
                ])
        return node_constructors

    def get_nodes_constructed_by_relations(self, node_types: Optional[List[str]] = None) -> Dict[
        str, List[NodeConstructor]]:
        node_constructors = {}
        for node in self.nodes:
            if node_types is None or node.type in node_types:
                for node_constructor in node.node_constructors:
                    if node_constructor.constructed_by_relation():
                        if node.type not in node_constructors:
                            node_constructors[node.type] = []
                        node_constructors[node.type].append(node_constructor)
        return node_constructors

    def get_entities_constructed_by_query(self):
        return [entity for entity in self.nodes if
                entity.constructor_type == "EntityConstructorByQuery"]

    def get_node_relation_constructors(self, rel_types: Optional[List[str]] = None):
        # get relations that are modeled as nodes
        rel_constructors = []
        # check for all diferent methods
        rel_constructors.extend(self.get_relations_constructed_by_nodes(rel_types, modeled_as_nodes=True))
        rel_constructors.extend(self.get_relations_constructed_by_relations(rel_types, modeled_as_nodes=True))
        rel_constructors.extend(self.get_relations_constructed_by_record(rel_types, modeled_as_nodes=True))
        rel_constructors.extend(self.get_relations_constructed_by_inference(rel_types, modeled_as_nodes=True))
        return rel_constructors

    def get_relations_constructed_by_nodes(self, rel_types: Optional[List[str]], modeled_as_nodes=False):
        return self._get_relations_constructed_by_specific_constructor(rel_types, constructor="nodes",
                                                                       modeled_as_nodes=modeled_as_nodes)

    def get_relations_constructed_by_relations(self, rel_types: Optional[List[str]], modeled_as_nodes=False):
        return self._get_relations_constructed_by_specific_constructor(rel_types, constructor="relations",
                                                                       modeled_as_nodes=modeled_as_nodes)

    def get_relations_constructed_by_record(self, rel_types: Optional[List[str]], modeled_as_nodes=False):
        return self._get_relations_constructed_by_specific_constructor(rel_types, constructor="record",
                                                                       modeled_as_nodes=modeled_as_nodes)

    def get_relations_constructed_by_inference(self, rel_types: Optional[List[str]], modeled_as_nodes=False):
        return self._get_relations_constructed_by_specific_constructor(rel_types, constructor="inference",
                                                                       modeled_as_nodes=modeled_as_nodes)

    def _get_relations_constructed_by_specific_constructor(self, rel_types, constructor, modeled_as_nodes):
        rel_constructors = []
        for relation in self.relations:
            if rel_types is None or relation.type in rel_types:
                if constructor == "nodes":
                    rel_constructors.extend(
                        [rel_constructor for rel_constructor in relation.relation_constructors if
                         rel_constructor.constructed_by_nodes(modeled_as_nodes)])
                elif constructor == "relations":
                    rel_constructors.extend(
                        [rel_constructor for rel_constructor in relation.relation_constructors if
                         rel_constructor.constructed_by_relations(modeled_as_nodes)])
                elif constructor == "record":
                    rel_constructors.extend(
                        [rel_constructor for rel_constructor in relation.relation_constructors if
                         rel_constructor.constructed_by_record(modeled_as_nodes)])
                elif constructor == "inference":
                    rel_constructors.extend(
                        [rel_constructor for rel_constructor in relation.relation_constructors if
                         rel_constructor.constructed_by_inference(modeled_as_nodes)])
        return rel_constructors

    def get_relations_derived_from_relations(self):
        return [relation for relation in self.relations if
                "RelationConstructorByRelations" in relation.constructor_type]

    def get_relations_derived_from_query(self):
        return [relation for relation in self.relations if
                relation.constructor_type == "RelationConstructorByQuery"]
