"""Base classes for schema representation.

Provides a normalized internal schema model that all external formats
(Swagger, Avro, DDL, etc.) are converted to.
"""

from enum import Enum
from typing import Any
from pydantic import BaseModel, Field


class SchemaFormat(str, Enum):
    """Supported schema formats."""

    # The canonical JSON format is the only supported import format.
    # Users should convert their schemas to this format using ChatGPT/Claude.
    # See docs/SCHEMA_IMPORT_FORMAT.md for details.
    CANONICAL = "canonical"

    # Legacy formats (deprecated - will be removed)
    SWAGGER = "swagger"
    OPENAPI = "openapi"
    JSON_SCHEMA = "jsonschema"
    AVRO = "avro"
    DDL = "ddl"
    PROTOBUF = "protobuf"


class FieldSchema(BaseModel):
    """Schema definition for a single field.

    This is the normalized representation that all external formats
    are converted to.
    """

    name: str = Field(..., description="Field name")
    type: str = Field(..., description="Field type (string, integer, float, boolean, array, object)")
    format: str | None = Field(default=None, description="Type format (uuid, email, date-time, etc.)")
    description: str | None = Field(default=None, description="Field description")
    required: bool = Field(default=False, description="Whether field is required")
    nullable: bool = Field(default=True, description="Whether field can be null")

    # Enum constraints
    enum: list[Any] | None = Field(default=None, description="Allowed enum values")

    # String constraints
    min_length: int | None = Field(default=None, description="Minimum string length")
    max_length: int | None = Field(default=None, description="Maximum string length")
    pattern: str | None = Field(default=None, description="Regex pattern for validation")

    # Numeric constraints
    minimum: float | None = Field(default=None, description="Minimum numeric value")
    maximum: float | None = Field(default=None, description="Maximum numeric value")
    exclusive_minimum: bool = Field(default=False, description="Whether minimum is exclusive")
    exclusive_maximum: bool = Field(default=False, description="Whether maximum is exclusive")

    # Array constraints
    items: "FieldSchema | None" = Field(default=None, description="Schema for array items")
    min_items: int | None = Field(default=None, description="Minimum array length")
    max_items: int | None = Field(default=None, description="Maximum array length")
    unique_items: bool = Field(default=False, description="Whether array items must be unique")

    # Object constraints
    properties: dict[str, "FieldSchema"] | None = Field(default=None, description="Nested object properties")

    # Default value
    default: Any | None = Field(default=None, description="Default value")

    # Example value (from schema)
    example: Any | None = Field(default=None, description="Example value")

    def to_generator_schema(self) -> dict[str, Any]:
        """Convert to the simple schema format used by generators.

        Returns a dict that can be used in profile options.schema
        """
        result: dict[str, Any] = {"type": self.type}

        if self.format:
            result["format"] = self.format
        if self.required:
            result["required"] = True
        if not self.nullable:
            result["nullable"] = False
        if self.enum:
            result["enum"] = self.enum
        if self.min_length is not None:
            result["min_length"] = self.min_length
        if self.max_length is not None:
            result["max_length"] = self.max_length
        if self.pattern:
            result["pattern"] = self.pattern
        if self.minimum is not None:
            result["minimum"] = self.minimum
        if self.maximum is not None:
            result["maximum"] = self.maximum
        if self.items:
            result["items"] = self.items.to_generator_schema()
        if self.properties:
            result["properties"] = {
                name: field.to_generator_schema()
                for name, field in self.properties.items()
            }
        if self.default is not None:
            result["default"] = self.default

        return result

    def to_simple_type(self) -> str:
        """Convert to simple type string for basic generator schema.

        Maps the detailed schema to simple types like 'uuid', 'email', 'integer'.
        """
        # Check format first for specific types
        if self.format:
            format_map = {
                "uuid": "uuid",
                "email": "email",
                "date": "date",
                "date-time": "datetime",
                "datetime": "datetime",
                "time": "time",
                "uri": "url",
                "url": "url",
                "hostname": "hostname",
                "ipv4": "ipv4",
                "ipv6": "ipv6",
                "phone": "phone",
            }
            if self.format.lower() in format_map:
                return format_map[self.format.lower()]

        # Map base types
        type_map = {
            "string": "string",
            "integer": "integer",
            "int": "integer",
            "int32": "integer",
            "int64": "integer",
            "long": "integer",
            "number": "float",
            "float": "float",
            "double": "float",
            "decimal": "float",
            "boolean": "boolean",
            "bool": "boolean",
            "array": "array",
            "object": "object",
        }

        return type_map.get(self.type.lower(), "string")


class SchemaDefinition(BaseModel):
    """Complete schema definition for an entity/table/message.

    This represents a full schema that can be converted to a profile.
    """

    name: str = Field(..., description="Schema/entity name")
    description: str | None = Field(default=None, description="Schema description")
    fields: list[FieldSchema] = Field(default_factory=list, description="Field definitions")

    # Source information
    source_format: SchemaFormat = Field(..., description="Original schema format")
    source_file: str | None = Field(default=None, description="Source file path")
    source_entity: str | None = Field(default=None, description="Entity name in source (table, message, etc.)")

    # Additional metadata
    metadata: dict[str, Any] = Field(default_factory=dict, description="Additional metadata")

    def get_required_fields(self) -> list[FieldSchema]:
        """Get all required fields."""
        return [f for f in self.fields if f.required]

    def get_field(self, name: str) -> FieldSchema | None:
        """Get a field by name."""
        for field in self.fields:
            if field.name == name:
                return field
        return None

    def to_generator_schema(self) -> dict[str, Any]:
        """Convert to generator-compatible schema dict.

        Returns a dict suitable for use in profile options.schema
        """
        return {
            field.name: field.to_generator_schema()
            for field in self.fields
        }

    def to_simple_schema(self) -> dict[str, str]:
        """Convert to simple schema dict with just field names and types.

        Returns a dict like: {"id": "uuid", "name": "string", "email": "email"}
        """
        return {
            field.name: field.to_simple_type()
            for field in self.fields
        }

    def to_profile_dict(
        self,
        dataset_type: str = "api",
        count: int = 100,
        output_format: str = "json",
        output_directory: str = "./output",
    ) -> dict[str, Any]:
        """Convert schema to a profile dictionary.

        This creates a complete profile dict that can be saved as YAML.
        """
        from datetime import datetime

        profile = {
            "name": f"{self.name.lower().replace(' ', '_')}_profile",
            "description": f"Generated from {self.source_format.value} schema '{self.name}'",
            "version": "1.0.0",
            "datasets": [
                {
                    "type": dataset_type,
                    "count": count,
                    "enabled": True,
                    "options": {
                        "schema": self.to_generator_schema(),
                    },
                }
            ],
            "output": {
                "format": output_format,
                "directory": output_directory,
                "filename_pattern": "{persona}_{dataset_type}_{timestamp}",
                "pretty_print": True,
                "include_metadata": True,
            },
            "metadata": {
                "source_file": self.source_file,
                "source_format": self.source_format.value,
                "source_entity": self.source_entity or self.name,
                "generated_at": datetime.now().isoformat(),
                "field_count": len(self.fields),
                "required_fields": [f.name for f in self.get_required_fields()],
            },
        }

        return profile
