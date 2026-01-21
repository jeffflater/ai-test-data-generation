"""Main schema parser module.

Provides a unified interface to parse schemas from various formats.
"""

from pathlib import Path
from typing import Any

from persona_platform.schemas.base import SchemaDefinition, SchemaFormat
from persona_platform.schemas.swagger import SwaggerParser
from persona_platform.schemas.jsonschema import JsonSchemaParser
from persona_platform.schemas.avro import AvroParser
from persona_platform.schemas.ddl import DDLParser
from persona_platform.schemas.protobuf import ProtobufParser


class SchemaParser:
    """Unified schema parser supporting multiple formats."""

    # File extensions to format mapping
    EXTENSION_FORMATS = {
        ".json": None,  # Could be swagger, jsonschema, or avro - need to detect
        ".yaml": None,  # Could be swagger - need to detect
        ".yml": None,   # Could be swagger - need to detect
        ".avsc": SchemaFormat.AVRO,
        ".sql": SchemaFormat.DDL,
        ".ddl": SchemaFormat.DDL,
        ".proto": SchemaFormat.PROTOBUF,
    }

    def __init__(
        self,
        content: str,
        format: SchemaFormat | str,
        source_file: str | None = None,
    ):
        """Initialize parser with content and format.

        Args:
            content: Schema content as string
            format: Schema format
            source_file: Optional source file path for metadata
        """
        if isinstance(format, str):
            format = SchemaFormat(format.lower())

        self.content = content
        self.format = format
        self.source_file = source_file
        self._parser = self._create_parser()

    @classmethod
    def from_file(
        cls,
        path: Path | str,
        format: SchemaFormat | str | None = None,
    ) -> "SchemaParser":
        """Create parser from a file.

        Args:
            path: Path to schema file
            format: Optional explicit format (auto-detected if not provided)

        Returns:
            Initialized SchemaParser
        """
        path = Path(path)
        content = path.read_text()

        if format is None:
            format = cls._detect_format(path, content)

        return cls(content, format, str(path))

    @classmethod
    def _detect_format(cls, path: Path, content: str) -> SchemaFormat:
        """Detect schema format from file extension and content.

        Args:
            path: File path
            content: File content

        Returns:
            Detected SchemaFormat
        """
        suffix = path.suffix.lower()

        # Check extension mapping first
        if suffix in cls.EXTENSION_FORMATS:
            detected = cls.EXTENSION_FORMATS[suffix]
            if detected:
                return detected

        # For JSON/YAML files, need content inspection
        if suffix in (".json", ".yaml", ".yml"):
            return cls._detect_json_yaml_format(content, suffix)

        raise ValueError(f"Cannot detect schema format for extension '{suffix}'")

    @classmethod
    def _detect_json_yaml_format(cls, content: str, suffix: str) -> SchemaFormat:
        """Detect format for JSON/YAML files.

        Args:
            content: File content
            suffix: File extension

        Returns:
            Detected SchemaFormat
        """
        import json
        import yaml

        try:
            if suffix in (".yaml", ".yml"):
                data = yaml.safe_load(content)
            else:
                data = json.loads(content)
        except Exception:
            raise ValueError("Failed to parse file content")

        if not isinstance(data, dict):
            raise ValueError("Schema must be a JSON/YAML object")

        # OpenAPI/Swagger detection
        if "openapi" in data or "swagger" in data:
            return SchemaFormat.SWAGGER

        # Avro detection
        if data.get("type") == "record" and "fields" in data:
            return SchemaFormat.AVRO

        # JSON Schema detection
        if "$schema" in data or "properties" in data or "type" in data:
            return SchemaFormat.JSON_SCHEMA

        # Default to JSON Schema
        return SchemaFormat.JSON_SCHEMA

    def _create_parser(self):
        """Create the appropriate parser for the format.

        Returns:
            Format-specific parser instance
        """
        if self.format in (SchemaFormat.SWAGGER, SchemaFormat.OPENAPI):
            return SwaggerParser.from_string(self.content, self._get_content_format())
        elif self.format == SchemaFormat.JSON_SCHEMA:
            return JsonSchemaParser.from_string(self.content)
        elif self.format == SchemaFormat.AVRO:
            return AvroParser.from_string(self.content)
        elif self.format == SchemaFormat.DDL:
            return DDLParser.from_string(self.content)
        elif self.format == SchemaFormat.PROTOBUF:
            return ProtobufParser.from_string(self.content)
        else:
            raise ValueError(f"Unsupported format: {self.format}")

    def _get_content_format(self) -> str:
        """Determine if content is JSON or YAML.

        Returns:
            'json' or 'yaml'
        """
        content_stripped = self.content.strip()
        if content_stripped.startswith("{"):
            return "json"
        return "yaml"

    def list_entities(self) -> list[str]:
        """List available entities (schemas, tables, messages) in the file.

        Returns:
            List of entity names
        """
        if self.format in (SchemaFormat.SWAGGER, SchemaFormat.OPENAPI):
            return self._parser.list_schemas()
        elif self.format == SchemaFormat.JSON_SCHEMA:
            defs = self._parser.list_definitions()
            return defs if defs else ["(root)"]
        elif self.format == SchemaFormat.AVRO:
            return self._parser.list_records()
        elif self.format == SchemaFormat.DDL:
            return self._parser.list_tables()
        elif self.format == SchemaFormat.PROTOBUF:
            return self._parser.list_messages()
        return []

    def parse(self, entity: str | None = None) -> SchemaDefinition:
        """Parse a schema entity.

        Args:
            entity: Optional entity name. If not provided, parses the first/root entity.

        Returns:
            Parsed SchemaDefinition
        """
        if self.format in (SchemaFormat.SWAGGER, SchemaFormat.OPENAPI):
            if entity:
                return self._parser.parse_schema(entity, self.source_file)
            # Return first schema if none specified
            schemas = self._parser.list_schemas()
            if schemas:
                return self._parser.parse_schema(schemas[0], self.source_file)
            raise ValueError("No schemas found in OpenAPI spec")

        elif self.format == SchemaFormat.JSON_SCHEMA:
            if entity and entity != "(root)":
                return self._parser.parse_definition(entity, self.source_file)
            return self._parser.parse(self.source_file)

        elif self.format == SchemaFormat.AVRO:
            if entity:
                return self._parser.parse_record(entity, self.source_file)
            return self._parser.parse(self.source_file)

        elif self.format == SchemaFormat.DDL:
            if entity:
                return self._parser.parse_table(entity, self.source_file)
            return self._parser.parse(self.source_file)

        elif self.format == SchemaFormat.PROTOBUF:
            if entity:
                return self._parser.parse_message(entity, self.source_file)
            return self._parser.parse(self.source_file)

        raise ValueError(f"Unsupported format: {self.format}")


def parse_schema_file(
    path: Path | str,
    format: SchemaFormat | str | None = None,
    entity: str | None = None,
) -> SchemaDefinition:
    """Convenience function to parse a schema file.

    Args:
        path: Path to schema file
        format: Optional explicit format (auto-detected if not provided)
        entity: Optional entity name to parse

    Returns:
        Parsed SchemaDefinition
    """
    parser = SchemaParser.from_file(path, format)
    return parser.parse(entity)
