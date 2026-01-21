"""JSON Schema parser.

Parses JSON Schema draft-04, draft-06, draft-07, and 2019-09/2020-12.
"""

import json
from pathlib import Path
from typing import Any

from persona_platform.schemas.base import FieldSchema, SchemaDefinition, SchemaFormat


class JsonSchemaParser:
    """Parser for JSON Schema specifications."""

    def __init__(self, schema: dict[str, Any]):
        """Initialize parser with a parsed schema.

        Args:
            schema: Parsed JSON Schema dict
        """
        self.schema = schema
        self._definitions = self._get_definitions()

    @classmethod
    def from_file(cls, path: Path | str) -> "JsonSchemaParser":
        """Load parser from a file.

        Args:
            path: Path to JSON Schema file

        Returns:
            Initialized parser
        """
        path = Path(path)
        content = path.read_text()
        schema = json.loads(content)
        return cls(schema)

    @classmethod
    def from_string(cls, content: str) -> "JsonSchemaParser":
        """Load parser from a string.

        Args:
            content: JSON Schema as string

        Returns:
            Initialized parser
        """
        schema = json.loads(content)
        return cls(schema)

    def _get_definitions(self) -> dict[str, Any]:
        """Get all definitions/defs from the schema.

        Returns:
            Dict of definition name to schema
        """
        # JSON Schema draft-07 and earlier use "definitions"
        # JSON Schema 2019-09+ use "$defs"
        definitions = self.schema.get("definitions", {})
        definitions.update(self.schema.get("$defs", {}))
        return definitions

    def list_definitions(self) -> list[str]:
        """List all available definition names.

        Returns:
            List of definition names
        """
        return list(self._definitions.keys())

    def parse(self, source_file: str | None = None) -> SchemaDefinition:
        """Parse the root schema.

        Args:
            source_file: Optional source file path for metadata

        Returns:
            Parsed SchemaDefinition
        """
        name = self.schema.get("title", "Schema")
        description = self.schema.get("description")

        if self.schema.get("type") == "object" or "properties" in self.schema:
            fields = self._parse_object_schema(self.schema)
        else:
            # Single field schema
            field = self._parse_field("value", self.schema, True)
            fields = [field]

        return SchemaDefinition(
            name=name,
            description=description,
            fields=fields,
            source_format=SchemaFormat.JSON_SCHEMA,
            source_file=source_file,
            source_entity=name,
            metadata={
                "schema_version": self.schema.get("$schema"),
                "id": self.schema.get("$id") or self.schema.get("id"),
            },
        )

    def parse_definition(
        self,
        definition_name: str,
        source_file: str | None = None,
    ) -> SchemaDefinition:
        """Parse a specific definition by name.

        Args:
            definition_name: Name of the definition to parse
            source_file: Optional source file path

        Returns:
            Parsed SchemaDefinition
        """
        if definition_name not in self._definitions:
            available = ", ".join(self._definitions.keys())
            raise ValueError(f"Definition '{definition_name}' not found. Available: {available}")

        def_schema = self._definitions[definition_name]
        fields = self._parse_object_schema(def_schema)

        return SchemaDefinition(
            name=definition_name,
            description=def_schema.get("description"),
            fields=fields,
            source_format=SchemaFormat.JSON_SCHEMA,
            source_file=source_file,
            source_entity=definition_name,
            metadata={
                "schema_version": self.schema.get("$schema"),
            },
        )

    def _parse_object_schema(self, schema: dict[str, Any]) -> list[FieldSchema]:
        """Parse an object schema into a list of FieldSchema.

        Args:
            schema: The object schema dict

        Returns:
            List of FieldSchema
        """
        fields = []
        required_fields = set(schema.get("required", []))
        properties = schema.get("properties", {})

        for name, prop in properties.items():
            prop = self._resolve_ref(prop)
            field = self._parse_field(name, prop, name in required_fields)
            fields.append(field)

        return fields

    def _parse_field(
        self,
        name: str,
        schema: dict[str, Any],
        required: bool,
    ) -> FieldSchema:
        """Parse a single field schema.

        Args:
            name: Field name
            schema: Field schema dict
            required: Whether the field is required

        Returns:
            FieldSchema
        """
        schema = self._resolve_ref(schema)

        # Handle type which can be a string or array
        field_type = schema.get("type", "string")
        if isinstance(field_type, list):
            # Multiple types allowed, take the first non-null
            field_type = next((t for t in field_type if t != "null"), "string")

        field_format = schema.get("format")

        # Handle nested objects
        properties = None
        if field_type == "object" and "properties" in schema:
            nested_fields = self._parse_object_schema(schema)
            properties = {f.name: f for f in nested_fields}

        # Handle arrays
        items = None
        if field_type == "array" and "items" in schema:
            items_schema = self._resolve_ref(schema["items"])
            items = self._parse_field("item", items_schema, False)

        # Check for nullable (type array with "null")
        type_value = schema.get("type")
        nullable = not required
        if isinstance(type_value, list) and "null" in type_value:
            nullable = True

        return FieldSchema(
            name=name,
            type=field_type,
            format=field_format,
            description=schema.get("description"),
            required=required,
            nullable=nullable,
            enum=schema.get("enum"),
            min_length=schema.get("minLength"),
            max_length=schema.get("maxLength"),
            pattern=schema.get("pattern"),
            minimum=schema.get("minimum"),
            maximum=schema.get("maximum"),
            exclusive_minimum=schema.get("exclusiveMinimum", False),
            exclusive_maximum=schema.get("exclusiveMaximum", False),
            items=items,
            min_items=schema.get("minItems"),
            max_items=schema.get("maxItems"),
            unique_items=schema.get("uniqueItems", False),
            properties=properties,
            default=schema.get("default"),
            example=schema.get("examples", [None])[0] if schema.get("examples") else schema.get("example"),
        )

    def _resolve_ref(self, schema: dict[str, Any]) -> dict[str, Any]:
        """Resolve a $ref reference.

        Args:
            schema: Schema that may contain $ref

        Returns:
            Resolved schema dict
        """
        if "$ref" not in schema:
            return schema

        ref = schema["$ref"]

        # Handle local references
        if ref.startswith("#/definitions/"):
            ref_name = ref.split("/")[-1]
            if ref_name in self._definitions:
                return self._definitions[ref_name]

        if ref.startswith("#/$defs/"):
            ref_name = ref.split("/")[-1]
            if ref_name in self._definitions:
                return self._definitions[ref_name]

        return schema
