"""Canonical JSON schema parser.

This is the only supported import format. Users should convert their schemas
(SQL, Avro, Protobuf, etc.) to this canonical JSON format using ChatGPT/Claude
before importing.

See docs/SCHEMA_IMPORT_FORMAT.md for the format specification and conversion prompts.
"""

import json
from pathlib import Path
from typing import Any

from persona_platform.schemas.base import FieldSchema, SchemaDefinition, SchemaFormat


class CanonicalSchemaParser:
    """Parser for the canonical JSON import format."""

    # Valid base types
    VALID_TYPES = {"string", "integer", "float", "boolean", "array", "object"}

    # Valid format hints
    VALID_FORMATS = {
        "uuid", "email", "date", "datetime", "date-time", "time",
        "url", "uri", "hostname", "ipv4", "ipv6", "phone"
    }

    def __init__(self, data: dict[str, Any], source_file: str | None = None):
        """Initialize parser with parsed JSON data.

        Args:
            data: Parsed JSON schema data
            source_file: Optional source file path for metadata
        """
        self.data = data
        self.source_file = source_file
        self._validate_structure()

    @classmethod
    def from_file(cls, path: Path | str) -> "CanonicalSchemaParser":
        """Create parser from a JSON file.

        Args:
            path: Path to JSON schema file

        Returns:
            Initialized CanonicalSchemaParser
        """
        path = Path(path)
        content = path.read_text()
        return cls.from_string(content, str(path))

    @classmethod
    def from_string(cls, content: str, source_file: str | None = None) -> "CanonicalSchemaParser":
        """Create parser from JSON string content.

        Args:
            content: JSON string
            source_file: Optional source file path

        Returns:
            Initialized CanonicalSchemaParser
        """
        try:
            data = json.loads(content)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON: {e}")

        return cls(data, source_file)

    def _validate_structure(self) -> None:
        """Validate the basic structure of the schema."""
        if not isinstance(self.data, dict):
            raise ValueError("Schema must be a JSON object")

        if "name" not in self.data:
            raise ValueError("Schema must have a 'name' field")

        if "fields" not in self.data:
            raise ValueError("Schema must have a 'fields' array")

        if not isinstance(self.data["fields"], list):
            raise ValueError("'fields' must be an array")

        if len(self.data["fields"]) == 0:
            raise ValueError("'fields' array must not be empty")

        # Validate each field
        for i, field in enumerate(self.data["fields"]):
            self._validate_field(field, f"fields[{i}]")

    def _validate_field(self, field: dict[str, Any], path: str) -> None:
        """Validate a field definition.

        Args:
            field: Field definition dict
            path: Path for error messages
        """
        if not isinstance(field, dict):
            raise ValueError(f"{path}: field must be an object")

        if "name" not in field:
            raise ValueError(f"{path}: field must have a 'name'")

        if "type" not in field:
            raise ValueError(f"{path}: field must have a 'type'")

        field_type = field["type"]
        if field_type not in self.VALID_TYPES:
            raise ValueError(
                f"{path}: invalid type '{field_type}'. "
                f"Valid types: {', '.join(sorted(self.VALID_TYPES))}"
            )

        # Validate format if present
        if "format" in field and field["format"] is not None:
            fmt = field["format"]
            if fmt not in self.VALID_FORMATS:
                raise ValueError(
                    f"{path}: invalid format '{fmt}'. "
                    f"Valid formats: {', '.join(sorted(self.VALID_FORMATS))}"
                )

        # Validate nested items for arrays
        if field_type == "array" and "items" in field:
            self._validate_field(field["items"], f"{path}.items")

        # Validate nested properties for objects
        if field_type == "object" and "properties" in field:
            props = field["properties"]
            if not isinstance(props, dict):
                raise ValueError(f"{path}.properties: must be an object")
            for name, prop in props.items():
                self._validate_field(prop, f"{path}.properties.{name}")

    def parse(self) -> SchemaDefinition:
        """Parse the schema into a SchemaDefinition.

        Returns:
            Parsed SchemaDefinition
        """
        fields = [self._parse_field(f) for f in self.data["fields"]]

        return SchemaDefinition(
            name=self.data["name"],
            description=self.data.get("description"),
            fields=fields,
            source_format=SchemaFormat.CANONICAL,
            source_file=self.source_file,
            source_entity=self.data["name"],
        )

    def _parse_field(self, field_data: dict[str, Any]) -> FieldSchema:
        """Parse a field definition into a FieldSchema.

        Args:
            field_data: Field definition dict

        Returns:
            Parsed FieldSchema
        """
        # Normalize format (date-time -> datetime)
        fmt = field_data.get("format")
        if fmt == "date-time":
            fmt = "datetime"
        elif fmt == "uri":
            fmt = "url"

        # Parse nested items for arrays
        items = None
        if field_data.get("type") == "array" and "items" in field_data:
            items = self._parse_field(field_data["items"])

        # Parse nested properties for objects
        properties = None
        if field_data.get("type") == "object" and "properties" in field_data:
            properties = {
                name: self._parse_field(prop)
                for name, prop in field_data["properties"].items()
            }

        return FieldSchema(
            name=field_data["name"],
            type=field_data["type"],
            format=fmt,
            description=field_data.get("description"),
            required=field_data.get("required", False),
            nullable=field_data.get("nullable", True),
            enum=field_data.get("enum"),
            min_length=field_data.get("min_length"),
            max_length=field_data.get("max_length"),
            pattern=field_data.get("pattern"),
            minimum=field_data.get("minimum"),
            maximum=field_data.get("maximum"),
            exclusive_minimum=field_data.get("exclusive_minimum", False),
            exclusive_maximum=field_data.get("exclusive_maximum", False),
            items=items,
            min_items=field_data.get("min_items"),
            max_items=field_data.get("max_items"),
            unique_items=field_data.get("unique_items", False),
            properties=properties,
            default=field_data.get("default"),
            example=field_data.get("example"),
        )


def is_canonical_format(content: str) -> bool:
    """Check if content appears to be in the canonical import format.

    Args:
        content: JSON string to check

    Returns:
        True if content looks like canonical format
    """
    try:
        data = json.loads(content)
    except json.JSONDecodeError:
        return False

    if not isinstance(data, dict):
        return False

    # Canonical format requires "name" and "fields" at the root
    return "name" in data and "fields" in data and isinstance(data.get("fields"), list)


def parse_canonical_schema(path: Path | str) -> SchemaDefinition:
    """Convenience function to parse a canonical schema file.

    Args:
        path: Path to schema file

    Returns:
        Parsed SchemaDefinition
    """
    parser = CanonicalSchemaParser.from_file(path)
    return parser.parse()
