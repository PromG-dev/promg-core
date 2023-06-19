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

        labels = labels.split(":")

        return Node(name=name, labels=labels, properties=properties,
                    where_condition=where_condition)

    def get_pattern(self, name: Optional[str] = None, with_brackets=False):

        node_pattern_str = "$node_name:$node_label"
        if name is None:
            node_pattern = Template(node_pattern_str).substitute(node_name=self.name,
                                                                 node_label=self.get_label_str())
        else:
            node_pattern = Template(node_pattern_str).substitute(node_name=name,
                                                                 node_label=self.get_label_str())

        if len(self.properties) > 0:
            properties_string = ",".join([prop.get_pattern() for prop in self.properties])
            node_pattern_str = "$node_pattern {$properties}"
            node_pattern = Template(node_pattern_str).substitute(node_pattern=node_pattern,
                                                                 properties=properties_string)
        elif self.where_condition != "":
            node_pattern_str = "$node_pattern WHERE $where_condition"
            node_pattern = Template(node_pattern_str).substitute(node_pattern=node_pattern,
                                                                 where_condition=self.where_condition)

        if with_brackets:
            node_pattern_str = "($node_pattern)"
            node_pattern = Template(node_pattern_str).substitute(node_pattern=node_pattern)

        return node_pattern

    def get_label_str(self, include_first_colon=False):
        if len(self.labels) > 0:
            return ":"*include_first_colon + ":".join(self.labels)
        return ""

    def __repr__(self):
        return self.get_pattern(with_brackets=True)


@dataclass()
class Relationship(ABC):
    relation_name: str
    relation_type: str
    from_node: Node
    to_node: Node
    properties: List[Any]
    has_direction: bool
    qi: Any

    @staticmethod
    def from_string(relation_description: str,
                    interpreter: Interpreter) -> Optional["Relationship"]:
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

        _has_direction = relation_directions[direction]["has_direction"]
        _from_node = Node.from_string(nodes[relation_directions[direction]["from_node"]])
        _to_node = Node.from_string(nodes[relation_directions[direction]["to_node"]])

        return Relationship(relation_name=_relation_name, relation_type=_relation_type,
                            from_node=_from_node, to_node=_to_node, properties=[], has_direction=_has_direction,
                            qi=interpreter.relationship_qi)

    def get_pattern(self):
        from_node_pattern = self.from_node.get_pattern()
        to_node_pattern = self.to_node.get_pattern()
        if self.relation_type != "":
            relationship_pattern = "$from_node - [$relation_name:$relation_type] -> $to_node" if self.has_direction \
                else "$from_node - [$relation_name:$relation_type] - $to_node"
            relationship_pattern = Template(relationship_pattern).substitute(from_node=from_node_pattern,
                                                                             to_node=to_node_pattern,
                                                                             relation_name=self.relation_name,
                                                                             relation_type=self.relation_type)
        else:
            relationship_pattern = "$from_node - [$relation_name] -> $to_node" if self.has_direction \
                else "$from_node - [$relation_name] - $to_node"
            relationship_pattern = Template(relationship_pattern).substitute(from_node=from_node_pattern,
                                                                             to_node=to_node_pattern,
                                                                             relation_name=self.relation_name)
        return relationship_pattern

    def __repr__(self):
        return self.get_pattern()


@dataclass
class RelationConstructorByNodes(ABC):
    from_node_label: str
    to_node_label: str
    foreign_key: str
    primary_key: str
    reversed: bool
    qi: Any

    @staticmethod
    def from_dict(obj: Any, interpreter: Interpreter) -> Optional["RelationConstructorByNodes"]:
        if obj is None:
            return None

        _from_node_label = obj.get("from_node_label")
        _to_node_label = obj.get("to_node_label")
        _foreign_key = obj.get("foreign_key")
        _primary_key = replace_undefined_value(obj.get("primary_key"), "ID")
        _reversed = replace_undefined_value(obj.get("reversed"), False)
        return RelationConstructorByNodes(from_node_label=_from_node_label, to_node_label=_to_node_label,
                                          foreign_key=_foreign_key, primary_key=_primary_key,
                                          reversed=_reversed, qi=interpreter.relation_constructor_by_nodes_qi)

    def get_id_attribute_from_from_node(self):
        return get_id_attribute_from_label(self.from_node_label)

    def get_id_attribute_from_to_node(self):
        return get_id_attribute_from_label(self.to_node_label)


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

        _constructed_by = RelationConstructorByNodes.from_dict(obj.get("constructed_by_nodes"),
                                                               interpreter)
        if _constructed_by is None:
            _constructed_by = RelationConstructorByRelations.from_dict(obj.get("constructed_by_relations"),
                                                                       interpreter)
        if _constructed_by is None:
            _constructed_by = RelationConstructorByQuery.from_dict(obj.get("constructed_by_query"), interpreter)

        _constructor_type = _constructed_by.__class__.__name__

        _include_properties = replace_undefined_value(obj.get("include_properties"), True)

        return Relation(_include, _type, constructed_by=_constructed_by, constructor_type=_constructor_type,
                        include_properties=_include_properties,
                        qi=interpreter.relation_qi)


@dataclass
class EntityConstructorByNode(ABC):
    node: Node

    @staticmethod
    def from_dict(obj: Any) -> Optional["EntityConstructorByNode"]:
        if obj is None:
            return None

        node = Node.from_string(obj.get("node"))

        return EntityConstructorByNode(node=node)


@dataclass
class EntityConstructorByRelation(ABC):
    relation: Relationship
    conditions: List[Condition]
    qi: Any

    @staticmethod
    def from_dict(obj: Any, interpreter: Interpreter = Interpreter) -> \
            Optional["EntityConstructorByRelation"]:
        if obj is None:
            return None

        _relation = Relationship.from_string(obj.get("relation_type"), interpreter)
        _conditions = create_list(Condition, obj.get("conditions"), interpreter)

        return EntityConstructorByRelation(relation=_relation, conditions=_conditions,
                                           qi=interpreter.entity_constructor_by_relation_qi)

    def get_relation_type(self):
        return self.relation.relation_type


@dataclass
class EntityConstructorByQuery(ABC):
    query: str
    qi: Any

    @staticmethod
    def from_dict(obj: Any, interpreter: Interpreter) -> Optional["EntityConstructorByQuery"]:
        if obj is None:
            return None

        _query = obj.get("query")

        return EntityConstructorByQuery(query=_query, qi=interpreter.entity_constructor_by_query_qi)


@dataclass
class Entity(ABC):
    type: str
    include: bool
    constructed_by: Union[EntityConstructorByNode, EntityConstructorByRelation, EntityConstructorByQuery]
    constructor_type: str
    result: Node
    labels: List[str]
    primary_keys: List[str]
    all_entity_attributes: List[str]
    entity_attributes_wo_primary_keys: List[str]
    corr: bool
    df: bool
    include_label_in_df: bool
    merge_duplicate_df: bool
    delete_parallel_df: bool
    qi: Any

    @staticmethod
    def from_dict(obj: Any, interpreter: Interpreter) -> Optional["Entity"]:

        _type = obj.get("type")
        if obj is None:
            return None
        _include = replace_undefined_value(obj.get("include"), True)
        if not _include:
            return None

        _constructed_by = EntityConstructorByNode.from_dict(obj.get("constructed_by_node"))
        if _constructed_by is None:
            _constructed_by = EntityConstructorByRelation.from_dict(obj.get("constructed_by_relation"),
                                                                    interpreter=interpreter)
        if _constructed_by is None:
            _constructed_by = EntityConstructorByQuery.from_dict(obj.get("constructed_by_query"),
                                                                 interpreter=interpreter)

        _constructor_type = _constructed_by.__class__.__name__
        _result = Node.from_string(obj.get("result"))
        _labels = replace_undefined_value(obj.get("labels"), [])
        _primary_keys = obj.get("primary_keys")
        # entity attributes may have primary keys (or not)
        _entity_attributes = replace_undefined_value(obj.get("entity_attributes"), [])
        # create a list of all entity attributes
        _all_entity_attributes = []
        if _primary_keys is not None:
            if len(_primary_keys) > 1:  # more than 1 primary key, also store the primary keys separately
                _all_entity_attributes = list(set(_entity_attributes + _primary_keys))
            else:
                # remove the primary keys from the entity attributes
                _all_entity_attributes = list(set(_entity_attributes).difference(set(_primary_keys)))
        # remove the primary keys
        _entity_attributes_wo_primary_keys = [attr for attr in _all_entity_attributes if attr not in _primary_keys]

        _corr = _include and replace_undefined_value(obj.get("corr"), False)
        _df = _corr and replace_undefined_value(obj.get("df"), False)
        _include_label_in_df = _df and replace_undefined_value(obj.get("include_label_in_df"), False)
        _merge_duplicate_df = _df and replace_undefined_value(obj.get("merge_duplicate_df"), False)

        _delete_parallel_df = _df and obj.get("delete_parallel_df")

        return Entity(include=_include, constructed_by=_constructed_by, constructor_type=_constructor_type,
                      type=_type, labels=_labels, primary_keys=_primary_keys,
                      all_entity_attributes=_all_entity_attributes,
                      entity_attributes_wo_primary_keys=_entity_attributes_wo_primary_keys,
                      corr=_corr, df=_df, include_label_in_df=_include_label_in_df,
                      merge_duplicate_df=_merge_duplicate_df,
                      delete_parallel_df=_delete_parallel_df,
                      qi=interpreter.entity_qi,
                      result=_result)

    def get_label_string(self):
        if self.result is not None:
            return self.result.get_label_str()
        return self.qi.get_label_string(self.labels)

    def get_labels(self):
        return ["Entity"] + self.labels

    def get_df_label(self):
        return self.qi.get_df_label(self.include_label_in_df, self.type)

    def get_composed_primary_id(self, node_name: str = "e"):
        if self.result is not None:
            return "+\"-\"+".join([f"{node_name}.{key}" for key in self.get_keys()])
        return "+\"-\"+".join([f"{node_name}.{key}" for key in self.primary_keys])

    def get_entity_attributes(self, node_name: str = "e"):
        return self.qi.get_entity_attributes(self.primary_keys, self.entity_attributes_wo_primary_keys,
                                             node_name)

    def get_entity_attributes_as_node_properties(self):
        if len(self.all_entity_attributes) > 0:
            return self.qi.get_entity_attributes_as_node_properties(self.all_entity_attributes)
        else:
            return ""

    def get_keys(self):
        keys = []
        for prop in self.result.properties:
            key = prop.ref_attribute
            keys.append(key)
        return keys

    def create_conditions(self, node_name):
        condition_list = []
        for condition in self.conditions:
            attribute_name = condition.attribute
            include_values = condition.values
            for value in include_values:
                condition_list.append(f'''{node_name}.{attribute_name} = "{value}"''')
        condition_string = " AND ".join(condition_list)
        return condition_string

    def get_where_condition(self, node_name: str = "e"):
        if self.result is not None:
            return " AND ".join(
                [f'''{node_name}.{key} IS NOT NULL AND {node_name}.{key} <> "Unknown"''' for key
                 in self.get_keys()])
        else:

            return " AND ".join(
                [f'''{node_name}.{key} IS NOT NULL AND {node_name}.{key} <> "Unknown"''' for key
                 in self.primary_keys])

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
                 entities: List[Entity], relations: List[Relation],
                 classes: List[Class], log: Log):
        self.name = name
        self.version = version
        self.entities = entities
        self.relations = relations
        self.classes = classes
        self.log = log

    def get_entity(self, entity_type) -> Optional[Entity]:
        for entity in self.entities:
            if entity_type == entity.type:
                return entity
        return None

    @staticmethod
    def from_dict(obj: Any, interpreter: Interpreter) -> Optional["SemanticHeader"]:
        if obj is None:
            return None
        _name = obj.get("name")
        _version = obj.get("version")
        _entities = create_list(Entity, obj.get("entities"), interpreter)
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
                entity.constructor_type == "EntityConstructorByNode"]

    def get_entities_constructed_by_relations(self):
        return [entity for entity in self.entities if
                entity.constructor_type == "EntityConstructorByRelation"]

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
