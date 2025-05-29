from string import Template
from typing import List, Optional


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
            components = attribute.split(".")
            node_name = components[0]
            node_attribute = components[1]

        return Property(attribute=attribute, value=value, node_name=node_name,
                        node_attribute=node_attribute, ref_node=ref_node, ref_attribute=ref_attribute)

    def get_pattern(self, is_set=False, name=None):
        if not is_set:
            return f"{self.attribute}: {self.value}"
        else:
            if name is None:
                return f"{self.attribute} = {self.value}"
            else:
                return f"{name}.{self.attribute} = COALESCE({name}.{self.attribute}, {self.value})"

    def __repr__(self):
        return self.get_pattern()


class Properties:
    def __init__(self, identifier_properties: List[Property],
                 required_properties: List[Property],
                 optional_properties: List[Property]):
        self.identifier_properties = identifier_properties
        self.required_properties = required_properties
        self.optional_properties = optional_properties

    def get_required_properties(self):
        return self.identifier_properties + self.required_properties

    def get_identifier_property_attributes(self):
        return [prop.attribute for prop in self.identifier_properties]

    @staticmethod
    def from_string(properties_str: str):
        """
        Interpret properties as a string and return it as a Properties object.
        :param properties_str:
        :return:
        """
        if properties_str is None:
            return None

        properties_str = properties_str.replace("}", "")
        properties = properties_str.split(",")

        # identify whether properties are identifier or other
        other_properties = [prop for prop in properties if "IDENTIFIER" not in prop]
        identifier_properties = [prop.replace("IDENTIFIER", "") for prop in properties if "IDENTIFIER" in prop]

        required_properties = [prop for prop in other_properties if "OPTIONAL" not in prop]
        optional_properties = [prop.replace("OPTIONAL", "") for prop in other_properties if "OPTIONAL" in prop]

        identifier_properties = [Property.from_string(prop) for prop in identifier_properties]
        required_properties = [Property.from_string(prop) for prop in required_properties]
        optional_properties = [Property.from_string(prop) for prop in optional_properties]

        return Properties(
            identifier_properties=identifier_properties,
            required_properties=required_properties,
            optional_properties=optional_properties
        )

    def get_string(self, with_brackets=False, with_optional=False):
        '''
        Return a string representation of the properties
        :param with_brackets:
        :param with_optional:
        :return:
        '''
        properties = [req_prop.get_pattern() for req_prop in self.get_required_properties()]
        if with_optional:
            properties += [f"OPTIONAL {prop.get_pattern()}" for prop in self.optional_properties]

        properties = ", ".join(properties)
        if with_brackets:
            property_string = "{$properties}"
            properties = Template(property_string).substitute(properties=properties)
        return properties

    def get_set_optional_properties_query(self, name):
        if len(self.optional_properties) == 0:
            return None
        return ",".join(
            [prop.get_pattern(is_set=True, name=name) for prop in self.optional_properties])

    def get_idt_properties_query(self, node_name):
        if self.identifier_properties is None:
            return None
        return ",".join(
            [f"{node_name}.{prop.attribute} as {prop.attribute}" for prop in self.identifier_properties])

    def has_required_properties(self, with_optional=False):
        if with_optional:
            return len(self.get_required_properties()) + len(self.optional_properties) > 0
        return len(self.get_required_properties()) > 0
