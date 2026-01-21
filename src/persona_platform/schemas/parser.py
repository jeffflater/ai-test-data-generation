"""Schema parser module.

Provides a simple interface to parse schemas from the canonical JSON format.

Users should convert their schemas (SQL, Avro, Protobuf, OpenAPI, etc.) to
the canonical format using ChatGPT/Claude before importing.

See docs/SCHEMA_IMPORT_FORMAT.md for the format specification and conversion prompts.
"""

from pathlib import Path

from persona_platform.schemas.base import SchemaDefinition
from persona_platform.schemas.canonical import (
    CanonicalSchemaParser,
    is_canonical_format,
)


class SchemaParser:
    """Schema parser for the canonical JSON format.

    This is the unified entry point for importing schemas. Only the canonical
    JSON format is supported. Users should convert their schemas from other
    formats (SQL, Avro, Protobuf, etc.) using ChatGPT/Claude.

    See docs/SCHEMA_IMPORT_FORMAT.md for format details and conversion prompts.
    """

    def __init__(
        self,
        content: str,
        source_file: str | None = None,
    ):
        """Initialize parser with content.

        Args:
            content: JSON schema content as string
            source_file: Optional source file path for metadata
        """
        self.content = content
        self.source_file = source_file
        self._parser = CanonicalSchemaParser.from_string(content, source_file)

    @classmethod
    def from_file(cls, path: Path | str) -> "SchemaParser":
        """Create parser from a file.

        Args:
            path: Path to JSON schema file

        Returns:
            Initialized SchemaParser

        Raises:
            ValueError: If file is not valid canonical JSON format
        """
        path = Path(path)

        if path.suffix.lower() not in (".json",):
            raise ValueError(
                f"Unsupported file extension '{path.suffix}'. "
                "Only .json files are supported. "
                "Convert your schema to the canonical JSON format first. "
                "See docs/SCHEMA_IMPORT_FORMAT.md for details."
            )

        content = path.read_text()

        if not is_canonical_format(content):
            raise ValueError(
                "File does not appear to be in the canonical import format. "
                "The file must be a JSON object with 'name' and 'fields' properties. "
                "Convert your schema using ChatGPT/Claude. "
                "See docs/SCHEMA_IMPORT_FORMAT.md for conversion prompts."
            )

        return cls(content, str(path))

    def parse(self) -> SchemaDefinition:
        """Parse the schema.

        Returns:
            Parsed SchemaDefinition
        """
        return self._parser.parse()


def parse_schema_file(path: Path | str) -> SchemaDefinition:
    """Convenience function to parse a schema file.

    Args:
        path: Path to canonical JSON schema file

    Returns:
        Parsed SchemaDefinition
    """
    parser = SchemaParser.from_file(path)
    return parser.parse()
