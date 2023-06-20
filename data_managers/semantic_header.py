import json
from abc import ABC
from pathlib import Path
from string import Template
from typing import List, Any, Optional, Union

from dataclasses import dataclass

from .interpreters import Interpreter
from ..utilities.auxiliary_functions import replace_undefined_value, create_list, get_id_attribute_from_label
import re


@dataclass
class Class:
    label: str
    aggregate_from_nodes: str
    class_identifiers: List[str]
    include_identifier_in_label: bool
    ids: List[str]
    qi: Any

    @staticmethod
    def from_dict(obj: Any, interpreter: Interpreter) -> Optional["Class"]:
        if obj is None:
            return None
        _label = obj.get("label")
        _aggregate_from_nodes = obj.get("aggregate_from_nodes")
        _class_identifiers = obj.get("class_identifiers")
        _include_identifier_in_label = replace_undefined_value(obj.get("include_identifier_in_label"), False)
        _ids = obj.get("ids")
        _query_interpreter = interpreter.class_qi
        return Class(_label, _aggregate_from_nodes, _class_identifiers, _include_identifier_in_label, _ids,
                     _query_interpreter)

    def get_condition(self, node_name="e"):
        return self.qi.get_condition(class_identifiers=self.class_identifiers, node_name=node_name)

    def get_group_by_statement(self, node_name="e"):
        return self.qi.get_group_by_statement(class_identifiers=self.class_identifiers,
                                              node_name=node_name)

    def get_class_properties(self) -> str:
        return self.qi.get_class_properties(class_identifiers=self.class_identifiers)

    def get_link_condition(self, class_node_name="c", event_node_name="e"):
        return self.qi.get_link_condition(class_identifiers=self.class_identifiers,
                                          class_node_name=class_node_name,
                                          event_node_name=event_node_name)

    def get_class_label(self):
        return self.qi.get_class_label(class_label=self.label, class_identifiers=self.class_identifiers,
                                       include_identifier_in_label=self.include_identifier_in_label)


@dataclass
class Condition:
    attribute: str
    values: List[Any]
    qi: Any

    @staticmethod
    def from_dict(obj: Any, query_interpreter) -> Optional["Condition"]:
        if obj is None:
            return None

        not_exist_properties = query_interpreter.get_not_exist_properties()
        _attribute = obj.get("attribute")
        _include_values = replace_undefined_value(obj.get("values"), not_exist_properties)
        _query_interpreter = query_interpreter
        return Condition(_attribute, _include_values, query_interpreter)


@dataclass
class Property:
    attribute: str
    value: str
    ref_node: Optional[str]
    ref_attribute: Optional[str]

    @staticmethod
    def from_string(property_description):
        if property_description is None:
            return None
        components = property_description.split(":")
        attribute = components[0]
        value = components[1]
        attribute = attribute.strip()
        value = value.strip()

        ref_node = None
        ref_attribute = None
        if "." in value:
            components = value.split(".")
            ref_node = components[0]
            ref_attribute = components[1]

        return Property(attribute=attribute, value=value, ref_node=ref_node, ref_attribute=ref_attribute)

    def get_pattern(self):
        return f"{self.attribute}: {self.value}"


@dataclass()
class Node(ABC):
    name: str
    labels: List[str]
    properties: List[Any]
    where_condition: str

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


@dataclass()
class Relationship(ABC):
    relation_name: str
    relation_type: str
    from_node: Node
    to_node: Node
    properties: List[Property]
    where_condition: str
    has_direction: bool

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

    @staticmethod
    def from_dict(obj: Any) -> Optional["RelationConstructorByNodes"]:
        if obj is None:
            return None

        _from_node = Node.from_string(obj.get("from_node"))
        _to_node = Node.from_string(obj.get("to_node"))

        return RelationConstructorByNodes(from_node=_from_node, to_node=_to_node)


class RelationshipOrNode(ABC):
    @staticmethod
    def from_string(relation_description: str, interpreter: Interpreter) -> Union["Relationship", "Node"]:
        if "-" in relation_description:
            return Relationship.from_string(relation_description, interpreter)
        else:
            return Node.from_string(relation_description, interpreter)


@dataclass
class RelationConstructorByRelations(ABC):
    antecedents: List[Relationship]
    consequent: Relationship
    from_node: Node
    to_node: Node
    qi: Any

    @staticmethod
    def from_dict(obj: Any, interpreter: Interpreter) -> \
            Optional["RelationConstructorByRelations"]:
        if obj is None:
            return None

        _antecedents = [RelationshipOrNode.from_string(y, interpreter) for y in obj.get("antecedents")]
        _consequent = Relationship.from_string(obj.get("consequent"), interpreter)

        _from_node = _consequent.from_node
        _to_node = _consequent.to_node

        return RelationConstructorByRelations(antecedents=_antecedents, consequent=_consequent,
                                              from_node=_from_node, to_node=_to_node,
                                              qi=interpreter.relation_constructor_by_relations_qi)

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
        return self.qi.get_antecedent_query(antecedents=self.antecedents)


@dataclass
class RelationConstructorByQuery(ABC):
    query: str
    qi: Any

    @staticmethod
    def from_dict(obj: Any, interpreter: Interpreter) -> Optional["RelationConstructorByQuery"]:
        if obj is None:
            return None

        _query = obj.get("query")

        return RelationConstructorByQuery(query=_query, qi=interpreter.relation_constructor_by_query_qi)


@dataclass
class Relation(ABC):
    include: bool
    type: str
    result: Relationship
    constructed_by: Union[RelationConstructorByNodes, RelationConstructorByRelations, RelationConstructorByQuery]
    constructor_type: str
    include_properties: bool
    qi: Any

    @staticmethod
    def from_dict(obj: Any, interpreter: Interpreter) -> Optional["Relation"]:
        if obj is None:
            return None
        _include = replace_undefined_value(obj.get("include"), True)
        if not _include:
            return None

        _type = obj.get("type")

        _constructed_by = RelationConstructorByNodes.from_dict(obj.get("constructed_by_nodes"))
        if _constructed_by is None:
            _constructed_by = RelationConstructorByRelations.from_dict(obj.get("constructed_by_relations"),
                                                                       interpreter)
        if _constructed_by is None:
            _constructed_by = RelationConstructorByQuery.from_dict(obj.get("constructed_by_query"), interpreter)

        _constructor_type = _constructed_by.__class__.__name__

        _result = Relationship.from_string(obj.get("result"))

        if _constructed_by.from_node.name != _result.from_node.name:
            raise ValueError(
                "Name of the from_node in the constructed_by_nodes does not match the name of the from_node"
                " in the result relationship")
        if _constructed_by.to_node.name != _result.to_node.name:
            raise ValueError(
                "Name of the to_node in the constructed_by_nodes does not match the name of the to_node"
                " in the result relationship")

        _include_properties = replace_undefined_value(obj.get("include_properties"), True)

        return Relation(_include, _type, constructed_by=_constructed_by, constructor_type=_constructor_type,
                        include_properties=_include_properties, result=_result,
                        qi=interpreter.relation_qi)

    def get_conditioned_node(self):
        if self.constructed_by.from_node.get_condition_string() != "":
            return "from_node"
        if self.constructed_by.to_node.get_condition_string() != "":
            return "to_node"
        else:
            return "none"


@dataclass
class NodesConstructorByQuery(ABC):
    query: str
    qi: Any

    @staticmethod
    def from_dict(obj: Any, interpreter: Interpreter) -> Optional["NodesConstructorByQuery"]:
        if obj is None:
            return None

        _query = obj.get("query")

        return NodesConstructorByQuery(query=_query, qi=interpreter.entity_constructor_by_query_qi)


class ConstructedNodes:
    def __init__(self, node_type: str, include: bool,
                 constructed_by: Union[Node, Relationship, NodesConstructorByQuery],
                 constructor_type: str, result: Node,
                 model_reified_relations: bool,
                 prevalence: bool,
                 observed: bool,
                 corr: bool,
                 df: bool,
                 include_label_in_df: bool,
                 merge_duplicate_df: bool,
                 delete_parallel_df: bool):
        self.node_type = node_type
        self.include = include
        self.constructed_by = constructed_by
        self.constructor_type = constructor_type
        self.result = result
        self.model_reified_relations = model_reified_relations
        self.prevalence = prevalence
        self.observed = observed
        self.corr = corr
        self.df = df
        self.include_label_in_df = include_label_in_df
        self.merge_duplicate_df = merge_duplicate_df
        self.delete_parallel_df = delete_parallel_df

    def __repr__(self):
        return self.result.__repr__()

    @staticmethod
    def from_dict(obj: Any, interpreter: Interpreter) -> Optional["ConstructedNodes"]:

        _type = obj.get("type")
        if obj is None:
            return None
        _include = replace_undefined_value(obj.get("include"), True)
        if not _include:
            return None

        _result = Node.from_string(obj.get("result"))
        _constructed_by = Node.from_string(obj.get("constructed_by_node"))
        if _constructed_by is None:
            _constructed_by = Relationship.from_string(obj.get("constructed_by_relation"))
        if _constructed_by is None:
            _constructed_by = NodesConstructorByQuery.from_dict(obj.get("constructed_by_query"),
                                                                interpreter=interpreter)

        _constructor_type = _constructed_by.__class__.__name__
        _model_reified_relations = replace_undefined_value(obj.get("model_reified_relations"), False)

        _prevalence = False
        if _constructor_type == "Node":
            _prevalence = "EventEntry" in _constructed_by.labels

        _observed = _include and replace_undefined_value(obj.get("observed"), False)
        _corr = _include and replace_undefined_value(obj.get("corr"), False)
        _df = _corr and replace_undefined_value(obj.get("df"), False)
        _include_label_in_df = _df and replace_undefined_value(obj.get("include_label_in_df"), False)
        _merge_duplicate_df = _df and replace_undefined_value(obj.get("merge_duplicate_df"), False)

        _delete_parallel_df = _df and obj.get("delete_parallel_df")

        constructed_node = ConstructedNodes(include=_include, constructed_by=_constructed_by,
                                            constructor_type=_constructor_type,
                                            node_type=_type, prevalence=_prevalence, observed=_observed, corr=_corr,
                                            df=_df,
                                            include_label_in_df=_include_label_in_df,
                                            merge_duplicate_df=_merge_duplicate_df,
                                            delete_parallel_df=_delete_parallel_df,
                                            result=_result, model_reified_relations=_model_reified_relations)

        # TODO check whether names match
        return constructed_node

    def get_label_string(self):
        return self.result.get_label_str()

    def get_labels(self):
        return self.result.labels

    def get_df_label(self):
        if self.include_label_in_df:
            return f'DF_{self.node_type.upper()}'
        else:
            return f'DF'

    def get_composed_primary_id(self, node_name: str = "e"):
        return "+\"-\"+".join([f"{node_name}.{key}" for key in self.get_keys()])

    def get_keys(self):
        keys = []
        for prop in self.result.properties:
            key = prop.ref_attribute
            keys.append(key)
        return keys

    def get_where_condition(self, node_name: str = "e"):
        return " AND ".join(
            [f'''{node_name}.{key} IS NOT NULL AND {node_name}.{key} <> "Unknown"''' for key
             in self.get_keys()])

    def get_where_condition_correlation(self, node_name: str = "e", node_name_id: str = "n"):
        primary_key_condition = f"{self.get_composed_primary_id(node_name)} = {node_name_id}.ID"
        # extra_conditions = self.create_conditions(node_name)
        return primary_key_condition


@dataclass
class Log:
    include: bool
    has: bool
    qi: Any

    @staticmethod
    def from_dict(obj: Any, interpreter: Interpreter) -> "Log":
        if obj is None:
            return Log(False, False, interpreter.log_qi)
        _include = replace_undefined_value(obj.get("include"), True)
        if not _include:
            return Log(False, False, interpreter.log_qi)
        _has = replace_undefined_value(obj.get("has"), True)
        return Log(_include, _has, qi=interpreter.log_qi)


class SemanticHeader(ABC):
    def __init__(self, name: str, version: str,
                 entities: List[ConstructedNodes], relations: List[Relation],
                 classes: List[Class], log: Log):
        self.name = name
        self.version = version
        self.entities = entities
        self.relations = relations
        self.classes = classes
        self.log = log

    def get_entity(self, entity_type) -> Optional[ConstructedNodes]:
        for entity in self.entities:
            if entity_type == entity.node_type:
                return entity
        return None

    @staticmethod
    def from_dict(obj: Any, interpreter: Interpreter) -> Optional["SemanticHeader"]:
        if obj is None:
            return None
        _name = obj.get("name")
        _version = obj.get("version")
        _entities = create_list(ConstructedNodes, obj.get("nodes"), interpreter)
        _relations = create_list(Relation, obj.get("relations"), interpreter)
        _classes = create_list(Class, obj.get("classes"), interpreter)
        _log = Log.from_dict(obj.get("log"), interpreter)
        return SemanticHeader(_name, _version, _entities, _relations,
                              _classes, _log)

    @staticmethod
    def create_semantic_header(path: Path, query_interpreter):
        with open(path) as f:
            json_semantic_header = json.load(f)

        semantic_header = SemanticHeader.from_dict(json_semantic_header, query_interpreter)
        return semantic_header

    def get_entities_constructed_by_nodes(self):
        return [entity for entity in self.entities if
                entity.constructor_type == "Node"]

    def get_entities_constructed_by_relations(self):
        return [entity for entity in self.entities if
                entity.constructor_type == "Relationship"]

    def get_entities_constructed_by_query(self):
        return [entity for entity in self.entities if
                entity.constructor_type == "EntityConstructorByQuery"]

    def get_relations_derived_from_nodes(self):
        return [relation for relation in self.relations if
                relation.constructor_type == "RelationConstructorByNodes"]

    def get_relations_derived_from_relations(self):
        return [relation for relation in self.relations if
                "RelationConstructorByRelations" in relation.constructor_type]

    def get_relations_derived_from_query(self):
        return [relation for relation in self.relations if
                relation.constructor_type == "RelationConstructorByQuery"]
