"""Avro schema parser.

Parses Apache Avro schema files (.avsc).
"""

import json
from pathlib import Path
from typing import Any

from persona_platform.schemas.base import FieldSchema, SchemaDefinition, SchemaFormat


class AvroParser:
    """Parser for Apache Avro schemas."""

    # Avro primitive types mapped to internal types
    PRIMITIVE_TYPES = {
        "null": "null",
        "boolean": "boolean",
        "int": "integer",
        "long": "integer",
        "float": "float",
        "double": "float",
        "bytes": "string",
        "string": "string",
    }

    # Avro logical types mapped to formats
    LOGICAL_TYPE_FORMATS = {
        "decimal": "decimal",
        "uuid": "uuid",
        "date": "date",
        "time-millis": "time",
        "time-micros": "time",
        "timestamp-millis": "datetime",
        "timestamp-micros": "datetime",
        "local-timestamp-millis": "datetime",
        "local-timestamp-micros": "datetime",
        "duration": "duration",
    }

    def __init__(self, schema: dict[str, Any]):
        """Initialize parser with a parsed schema.

        Args:
            schema: Parsed Avro schema dict
        """
        self.schema = schema
        self._named_types: dict[str, dict[str, Any]] = {}
        self._collect_named_types(schema)

    @classmethod
    def from_file(cls, path: Path | str) -> "AvroParser":
        """Load parser from a file.

        Args:
            path: Path to .avsc file

        Returns:
            Initialized parser
        """
        path = Path(path)
        content = path.read_text()
        schema = json.loads(content)
        return cls(schema)

    @classmethod
    def from_string(cls, content: str) -> "AvroParser":
        """Load parser from a string.

        Args:
            content: Avro schema as JSON string

        Returns:
            Initialized parser
        """
        schema = json.loads(content)
        return cls(schema)

    def _collect_named_types(self, schema: dict[str, Any] | list | str) -> None:
        """Recursively collect all named types (records, enums, fixed).

        Args:
            schema: Schema to traverse
        """
        if isinstance(schema, str):
            return

        if isinstance(schema, list):
            for item in schema:
                self._collect_named_types(item)
            return

        if isinstance(schema, dict):
            schema_type = schema.get("type")

            # Named types
            if schema_type in ("record", "enum", "fixed"):
                name = schema.get("name", "")
                namespace = schema.get("namespace", "")
                full_name = f"{namespace}.{name}" if namespace else name
                self._named_types[name] = schema
                self._named_types[full_name] = schema

            # Recurse into fields
            if schema_type == "record":
                for field in schema.get("fields", []):
                    self._collect_named_types(field.get("type", {}))

            # Recurse into array/map items
            if schema_type == "array":
                self._collect_named_types(schema.get("items", {}))
            if schema_type == "map":
                self._collect_named_types(schema.get("values", {}))

    def list_records(self) -> list[str]:
        """List all record type names.

        Returns:
            List of record names
        """
        return [
            name for name, schema in self._named_types.items()
            if isinstance(schema, dict) and schema.get("type") == "record"
            and "." not in name  # Return simple names only
        ]

    def parse(self, source_file: str | None = None) -> SchemaDefinition:
        """Parse the root schema.

        Args:
            source_file: Optional source file path for metadata

        Returns:
            Parsed SchemaDefinition
        """
        if self.schema.get("type") != "record":
            raise ValueError("Root Avro schema must be a record type")

        return self._parse_record(self.schema, source_file)

    def parse_record(
        self,
        record_name: str,
        source_file: str | None = None,
    ) -> SchemaDefinition:
        """Parse a specific record by name.

        Args:
            record_name: Name of the record to parse
            source_file: Optional source file path

        Returns:
            Parsed SchemaDefinition
        """
        if record_name not in self._named_types:
            available = ", ".join(self.list_records())
            raise ValueError(f"Record '{record_name}' not found. Available: {available}")

        record_schema = self._named_types[record_name]
        return self._parse_record(record_schema, source_file)

    def _parse_record(
        self,
        record_schema: dict[str, Any],
        source_file: str | None = None,
    ) -> SchemaDefinition:
        """Parse a record schema into SchemaDefinition.

        Args:
            record_schema: Avro record schema
            source_file: Optional source file path

        Returns:
            Parsed SchemaDefinition
        """
        name = record_schema.get("name", "Record")
        namespace = record_schema.get("namespace", "")
        doc = record_schema.get("doc")

        fields = []
        for field in record_schema.get("fields", []):
            parsed_field = self._parse_field(field)
            fields.append(parsed_field)

        return SchemaDefinition(
            name=name,
            description=doc,
            fields=fields,
            source_format=SchemaFormat.AVRO,
            source_file=source_file,
            source_entity=f"{namespace}.{name}" if namespace else name,
            metadata={
                "namespace": namespace,
                "aliases": record_schema.get("aliases", []),
            },
        )

    def _parse_field(self, field: dict[str, Any]) -> FieldSchema:
        """Parse an Avro field into FieldSchema.

        Args:
            field: Avro field definition

        Returns:
            Parsed FieldSchema
        """
        name = field.get("name", "")
        doc = field.get("doc")
        default = field.get("default")
        field_type = field.get("type")

        # Parse the type
        type_info = self._parse_type(field_type)

        return FieldSchema(
            name=name,
            type=type_info["type"],
            format=type_info.get("format"),
            description=doc,
            required=not type_info.get("nullable", False),
            nullable=type_info.get("nullable", False),
            enum=type_info.get("enum"),
            items=type_info.get("items"),
            properties=type_info.get("properties"),
            default=default,
            min_length=type_info.get("min_length"),
            max_length=type_info.get("max_length"),
        )

    def _parse_type(self, avro_type: str | list | dict) -> dict[str, Any]:
        """Parse an Avro type into internal type info.

        Args:
            avro_type: Avro type definition

        Returns:
            Dict with type info
        """
        # Union type (e.g., ["null", "string"])
        if isinstance(avro_type, list):
            return self._parse_union_type(avro_type)

        # Primitive type as string
        if isinstance(avro_type, str):
            if avro_type in self.PRIMITIVE_TYPES:
                return {"type": self.PRIMITIVE_TYPES[avro_type]}
            # Named type reference
            if avro_type in self._named_types:
                return self._parse_type(self._named_types[avro_type])
            return {"type": "string"}

        # Complex type as dict
        if isinstance(avro_type, dict):
            return self._parse_complex_type(avro_type)

        return {"type": "string"}

    def _parse_union_type(self, types: list) -> dict[str, Any]:
        """Parse an Avro union type.

        Args:
            types: List of union types

        Returns:
            Dict with type info
        """
        nullable = "null" in types
        non_null_types = [t for t in types if t != "null"]

        if not non_null_types:
            return {"type": "null", "nullable": True}

        # Take the first non-null type
        first_type = non_null_types[0]
        type_info = self._parse_type(first_type)
        type_info["nullable"] = nullable

        return type_info

    def _parse_complex_type(self, type_schema: dict[str, Any]) -> dict[str, Any]:
        """Parse a complex Avro type.

        Args:
            type_schema: Complex type schema dict

        Returns:
            Dict with type info
        """
        type_name = type_schema.get("type", "")

        # Check for logical type
        logical_type = type_schema.get("logicalType")
        if logical_type:
            format_name = self.LOGICAL_TYPE_FORMATS.get(logical_type)
            base_type = self.PRIMITIVE_TYPES.get(type_name, "string")
            return {
                "type": base_type,
                "format": format_name,
            }

        # Record type
        if type_name == "record":
            nested_fields = {}
            for field in type_schema.get("fields", []):
                parsed = self._parse_field(field)
                nested_fields[parsed.name] = parsed
            return {
                "type": "object",
                "properties": nested_fields,
            }

        # Enum type
        if type_name == "enum":
            return {
                "type": "string",
                "enum": type_schema.get("symbols", []),
            }

        # Array type
        if type_name == "array":
            items_type = type_schema.get("items")
            items_info = self._parse_type(items_type)
            items_field = FieldSchema(
                name="item",
                type=items_info.get("type", "string"),
                format=items_info.get("format"),
                enum=items_info.get("enum"),
            )
            return {
                "type": "array",
                "items": items_field,
            }

        # Map type
        if type_name == "map":
            values_type = type_schema.get("values")
            values_info = self._parse_type(values_type)
            return {
                "type": "object",
                "format": "map",
            }

        # Fixed type
        if type_name == "fixed":
            size = type_schema.get("size", 0)
            return {
                "type": "string",
                "min_length": size,
                "max_length": size,
            }

        # Fallback to primitive
        if type_name in self.PRIMITIVE_TYPES:
            return {"type": self.PRIMITIVE_TYPES[type_name]}

        return {"type": "string"}
