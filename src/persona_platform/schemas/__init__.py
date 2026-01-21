"""Schema parsing module for importing external schema definitions.

Only the canonical JSON format is supported. Users should convert their schemas
from other formats (SQL, Avro, Protobuf, OpenAPI, GraphQL, etc.) using
ChatGPT/Claude before importing.

See docs/SCHEMA_IMPORT_FORMAT.md for:
- Format specification
- Ready-to-use conversion prompts for common formats
- Examples

Example usage:
    from persona_platform.schemas import parse_schema_file

    # Parse a canonical JSON schema
    schema = parse_schema_file("users.import.json")

    # Convert to a profile
    profile_dict = schema.to_profile_dict()
"""

from persona_platform.schemas.base import (
    FieldSchema,
    SchemaDefinition,
    SchemaFormat,
)
from persona_platform.schemas.parser import SchemaParser, parse_schema_file
from persona_platform.schemas.canonical import (
    CanonicalSchemaParser,
    is_canonical_format,
    parse_canonical_schema,
)

__all__ = [
    "FieldSchema",
    "SchemaDefinition",
    "SchemaFormat",
    "SchemaParser",
    "parse_schema_file",
    "CanonicalSchemaParser",
    "is_canonical_format",
    "parse_canonical_schema",
]
