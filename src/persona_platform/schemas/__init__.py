"""Schema parsing module for importing external schema definitions.

Supports:
- Swagger/OpenAPI
- JSON Schema
- Avro
- SQL DDL
- Protobuf
"""

from persona_platform.schemas.base import (
    FieldSchema,
    SchemaDefinition,
    SchemaFormat,
)
from persona_platform.schemas.parser import SchemaParser, parse_schema_file

__all__ = [
    "FieldSchema",
    "SchemaDefinition",
    "SchemaFormat",
    "SchemaParser",
    "parse_schema_file",
]
