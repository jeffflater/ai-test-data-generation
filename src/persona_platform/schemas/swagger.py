"""Swagger/OpenAPI schema parser.

Parses OpenAPI 2.0 (Swagger) and OpenAPI 3.x specifications.
"""

import json
from pathlib import Path
from typing import Any

import yaml

from persona_platform.schemas.base import FieldSchema, SchemaDefinition, SchemaFormat


class SwaggerParser:
    """Parser for Swagger/OpenAPI specifications."""

    def __init__(self, spec: dict[str, Any]):
        """Initialize parser with a parsed spec.

        Args:
            spec: Parsed OpenAPI specification dict
        """
        self.spec = spec
        self._is_openapi3 = spec.get("openapi", "").startswith("3.")

    @classmethod
    def from_file(cls, path: Path | str) -> "SwaggerParser":
        """Load parser from a file.

        Args:
            path: Path to JSON or YAML OpenAPI spec

        Returns:
            Initialized parser
        """
        path = Path(path)
        content = path.read_text()

        if path.suffix.lower() in (".yaml", ".yml"):
            spec = yaml.safe_load(content)
        else:
            spec = json.loads(content)

        return cls(spec)

    @classmethod
    def from_string(cls, content: str, format: str = "json") -> "SwaggerParser":
        """Load parser from a string.

        Args:
            content: OpenAPI spec as string
            format: "json" or "yaml"

        Returns:
            Initialized parser
        """
        if format.lower() in ("yaml", "yml"):
            spec = yaml.safe_load(content)
        else:
            spec = json.loads(content)

        return cls(spec)

    def list_schemas(self) -> list[str]:
        """List all available schema names in the spec.

        Returns:
            List of schema names
        """
        if self._is_openapi3:
            schemas = self.spec.get("components", {}).get("schemas", {})
        else:
            schemas = self.spec.get("definitions", {})

        return list(schemas.keys())

    def list_endpoints(self) -> list[tuple[str, str]]:
        """List all endpoints in the spec.

        Returns:
            List of (method, path) tuples
        """
        endpoints = []
        for path, methods in self.spec.get("paths", {}).items():
            for method in methods:
                if method.upper() in ("GET", "POST", "PUT", "PATCH", "DELETE"):
                    endpoints.append((method.upper(), path))
        return endpoints

    def parse_schema(self, schema_name: str, source_file: str | None = None) -> SchemaDefinition:
        """Parse a specific schema by name.

        Args:
            schema_name: Name of the schema to parse
            source_file: Optional source file path for metadata

        Returns:
            Parsed SchemaDefinition
        """
        if self._is_openapi3:
            schemas = self.spec.get("components", {}).get("schemas", {})
        else:
            schemas = self.spec.get("definitions", {})

        if schema_name not in schemas:
            available = ", ".join(schemas.keys())
            raise ValueError(f"Schema '{schema_name}' not found. Available: {available}")

        schema_data = schemas[schema_name]
        fields = self._parse_object_schema(schema_data, schemas)

        return SchemaDefinition(
            name=schema_name,
            description=schema_data.get("description"),
            fields=fields,
            source_format=SchemaFormat.SWAGGER,
            source_file=source_file,
            source_entity=schema_name,
            metadata={
                "openapi_version": self.spec.get("openapi") or self.spec.get("swagger"),
                "info": self.spec.get("info", {}),
            },
        )

    def parse_endpoint_request(
        self,
        method: str,
        path: str,
        source_file: str | None = None,
    ) -> SchemaDefinition | None:
        """Parse the request body schema for an endpoint.

        Args:
            method: HTTP method
            path: Endpoint path
            source_file: Optional source file path

        Returns:
            Parsed SchemaDefinition or None if no request body
        """
        paths = self.spec.get("paths", {})
        if path not in paths:
            raise ValueError(f"Path '{path}' not found")

        endpoint = paths[path].get(method.lower())
        if not endpoint:
            raise ValueError(f"Method '{method}' not found for path '{path}'")

        # Get request body schema
        if self._is_openapi3:
            request_body = endpoint.get("requestBody", {})
            content = request_body.get("content", {})
            json_content = content.get("application/json", {})
            schema = json_content.get("schema", {})
        else:
            # OpenAPI 2.0 uses parameters with in: body
            for param in endpoint.get("parameters", []):
                if param.get("in") == "body":
                    schema = param.get("schema", {})
                    break
            else:
                return None

        if not schema:
            return None

        # Resolve $ref if present
        if self._is_openapi3:
            schemas = self.spec.get("components", {}).get("schemas", {})
        else:
            schemas = self.spec.get("definitions", {})

        schema = self._resolve_ref(schema, schemas)

        if schema.get("type") != "object" and "properties" not in schema:
            return None

        fields = self._parse_object_schema(schema, schemas)
        name = f"{method.upper()}_{path.replace('/', '_').strip('_')}_request"

        return SchemaDefinition(
            name=name,
            description=endpoint.get("summary") or endpoint.get("description"),
            fields=fields,
            source_format=SchemaFormat.SWAGGER,
            source_file=source_file,
            source_entity=f"{method.upper()} {path}",
            metadata={
                "endpoint": path,
                "method": method.upper(),
                "operation_id": endpoint.get("operationId"),
            },
        )

    def _parse_object_schema(
        self,
        schema: dict[str, Any],
        all_schemas: dict[str, Any],
    ) -> list[FieldSchema]:
        """Parse an object schema into a list of FieldSchema.

        Args:
            schema: The object schema dict
            all_schemas: All available schemas for reference resolution

        Returns:
            List of FieldSchema
        """
        fields = []
        required_fields = set(schema.get("required", []))
        properties = schema.get("properties", {})

        for name, prop in properties.items():
            # Resolve references
            prop = self._resolve_ref(prop, all_schemas)
            field = self._parse_field(name, prop, name in required_fields, all_schemas)
            fields.append(field)

        return fields

    def _parse_field(
        self,
        name: str,
        schema: dict[str, Any],
        required: bool,
        all_schemas: dict[str, Any],
    ) -> FieldSchema:
        """Parse a single field schema.

        Args:
            name: Field name
            schema: Field schema dict
            required: Whether the field is required
            all_schemas: All schemas for reference resolution

        Returns:
            FieldSchema
        """
        schema = self._resolve_ref(schema, all_schemas)

        field_type = schema.get("type", "string")
        field_format = schema.get("format")

        # Handle nested objects
        properties = None
        if field_type == "object" and "properties" in schema:
            nested_fields = self._parse_object_schema(schema, all_schemas)
            properties = {f.name: f for f in nested_fields}

        # Handle arrays
        items = None
        if field_type == "array" and "items" in schema:
            items_schema = self._resolve_ref(schema["items"], all_schemas)
            items = self._parse_field("item", items_schema, False, all_schemas)

        return FieldSchema(
            name=name,
            type=field_type,
            format=field_format,
            description=schema.get("description"),
            required=required,
            nullable=schema.get("nullable", not required),
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
            example=schema.get("example"),
        )

    def _resolve_ref(
        self,
        schema: dict[str, Any],
        all_schemas: dict[str, Any],
    ) -> dict[str, Any]:
        """Resolve a $ref reference.

        Args:
            schema: Schema that may contain $ref
            all_schemas: All available schemas

        Returns:
            Resolved schema dict
        """
        if "$ref" not in schema:
            return schema

        ref = schema["$ref"]
        # Handle #/components/schemas/Name or #/definitions/Name
        if ref.startswith("#/"):
            parts = ref.split("/")
            ref_name = parts[-1]
            if ref_name in all_schemas:
                return all_schemas[ref_name]

        return schema
