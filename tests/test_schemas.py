"""Tests for schema parsing functionality.

Tests cover:
- FieldSchema and SchemaDefinition models
- Canonical JSON schema parser
- Schema to profile conversion
"""

import json
import tempfile
from pathlib import Path

import pytest
import yaml

from persona_platform.schemas import (
    FieldSchema,
    SchemaDefinition,
    SchemaFormat,
    SchemaParser,
    parse_schema_file,
    CanonicalSchemaParser,
    is_canonical_format,
)


# Test data paths
SCHEMAS_DIR = Path(__file__).parent.parent / "examples" / "schemas"
CANONICAL_FILE = SCHEMAS_DIR / "users.import.json"


# =============================================================================
# FieldSchema and SchemaDefinition Tests
# =============================================================================

class TestFieldSchema:
    """Tests for FieldSchema model."""

    def test_field_schema_basic(self):
        """Test basic field schema creation."""
        field = FieldSchema(name="username", type="string")
        assert field.name == "username"
        assert field.type == "string"
        assert field.required is False
        assert field.nullable is True

    def test_field_schema_with_constraints(self):
        """Test field schema with constraints."""
        field = FieldSchema(
            name="age",
            type="integer",
            required=True,
            nullable=False,
            minimum=0,
            maximum=150,
        )
        assert field.name == "age"
        assert field.type == "integer"
        assert field.required is True
        assert field.nullable is False
        assert field.minimum == 0
        assert field.maximum == 150

    def test_field_schema_with_enum(self):
        """Test field schema with enum values."""
        field = FieldSchema(
            name="role",
            type="string",
            enum=["admin", "user", "guest"],
        )
        assert field.enum == ["admin", "user", "guest"]

    def test_field_schema_with_format(self):
        """Test field schema with format."""
        field = FieldSchema(
            name="email",
            type="string",
            format="email",
        )
        assert field.format == "email"

    def test_field_schema_with_nested_properties(self):
        """Test field schema with nested object properties."""
        street_field = FieldSchema(name="street", type="string")
        city_field = FieldSchema(name="city", type="string")

        address_field = FieldSchema(
            name="address",
            type="object",
            properties={"street": street_field, "city": city_field},
        )

        assert address_field.type == "object"
        assert "street" in address_field.properties
        assert address_field.properties["street"].type == "string"

    def test_field_schema_with_array_items(self):
        """Test field schema with array items."""
        items_field = FieldSchema(name="item", type="string")
        tags_field = FieldSchema(
            name="tags",
            type="array",
            items=items_field,
        )

        assert tags_field.type == "array"
        assert tags_field.items.type == "string"

    def test_field_to_dict(self):
        """Test field schema to dict conversion."""
        field = FieldSchema(
            name="email",
            type="string",
            format="email",
            required=True,
        )
        d = field.to_generator_schema()

        assert d["type"] == "string"
        assert d["format"] == "email"
        assert d["required"] is True


class TestSchemaDefinition:
    """Tests for SchemaDefinition model."""

    def test_schema_definition_basic(self):
        """Test basic schema definition creation."""
        fields = [
            FieldSchema(name="id", type="string", required=True),
            FieldSchema(name="name", type="string"),
        ]
        schema = SchemaDefinition(
            name="User",
            fields=fields,
            source_format=SchemaFormat.CANONICAL,
        )

        assert schema.name == "User"
        assert len(schema.fields) == 2
        assert schema.source_format == SchemaFormat.CANONICAL

    def test_schema_definition_to_profile_dict(self):
        """Test schema definition to profile dict conversion."""
        fields = [
            FieldSchema(name="id", type="string", format="uuid", required=True),
            FieldSchema(name="email", type="string", format="email"),
        ]
        schema = SchemaDefinition(
            name="User",
            description="A user entity",
            fields=fields,
            source_format=SchemaFormat.CANONICAL,
            source_file="users.import.json",
        )

        profile = schema.to_profile_dict(dataset_type="api", count=50)

        assert "datasets" in profile
        assert profile["datasets"][0]["type"] == "api"
        assert profile["datasets"][0]["count"] == 50
        assert "schema" in profile["datasets"][0]["options"]

    def test_schema_definition_required_fields(self):
        """Test schema definition tracks required fields in metadata."""
        fields = [
            FieldSchema(name="id", type="string", required=True),
            FieldSchema(name="email", type="string", required=True),
            FieldSchema(name="name", type="string", required=False),
        ]
        schema = SchemaDefinition(
            name="User",
            fields=fields,
            source_format=SchemaFormat.CANONICAL,
        )

        profile = schema.to_profile_dict()
        assert "id" in profile["metadata"]["required_fields"]
        assert "email" in profile["metadata"]["required_fields"]
        assert "name" not in profile["metadata"]["required_fields"]


# =============================================================================
# Canonical Schema Parser Tests
# =============================================================================

class TestCanonicalSchemaParser:
    """Tests for canonical JSON schema parser."""

    @pytest.fixture
    def parser(self):
        """Create parser from test file."""
        return CanonicalSchemaParser.from_file(CANONICAL_FILE)

    @pytest.fixture
    def sample_schema(self):
        """Sample canonical schema for testing."""
        return {
            "name": "users",
            "description": "User accounts",
            "fields": [
                {"name": "id", "type": "string", "format": "uuid", "required": True},
                {"name": "email", "type": "string", "format": "email", "required": True},
                {"name": "username", "type": "string", "min_length": 3, "max_length": 50},
                {"name": "age", "type": "integer", "minimum": 0, "maximum": 150},
                {"name": "role", "type": "string", "enum": ["admin", "user", "guest"]},
                {"name": "is_active", "type": "boolean", "default": True},
            ]
        }

    def test_from_file(self, parser):
        """Test loading parser from file."""
        assert parser is not None

    def test_from_string(self, sample_schema):
        """Test loading parser from string."""
        content = json.dumps(sample_schema)
        parser = CanonicalSchemaParser.from_string(content)
        assert parser is not None

    def test_parse_schema(self, parser):
        """Test parsing the schema."""
        schema = parser.parse()

        assert schema.name == "users"
        assert schema.source_format == SchemaFormat.CANONICAL

        field_names = [f.name for f in schema.fields]
        assert "id" in field_names
        assert "email" in field_names
        assert "username" in field_names

    def test_parse_required_fields(self, parser):
        """Test that required fields are properly marked."""
        schema = parser.parse()

        fields_by_name = {f.name: f for f in schema.fields}
        assert fields_by_name["id"].required is True
        assert fields_by_name["email"].required is True

    def test_parse_field_types(self, sample_schema):
        """Test that field types are correctly parsed."""
        parser = CanonicalSchemaParser(sample_schema)
        schema = parser.parse()

        fields_by_name = {f.name: f for f in schema.fields}
        assert fields_by_name["id"].type == "string"
        assert fields_by_name["age"].type == "integer"
        assert fields_by_name["is_active"].type == "boolean"

    def test_parse_formats(self, sample_schema):
        """Test that formats are extracted."""
        parser = CanonicalSchemaParser(sample_schema)
        schema = parser.parse()

        fields_by_name = {f.name: f for f in schema.fields}
        assert fields_by_name["id"].format == "uuid"
        assert fields_by_name["email"].format == "email"

    def test_parse_enum_values(self, sample_schema):
        """Test that enum values are extracted."""
        parser = CanonicalSchemaParser(sample_schema)
        schema = parser.parse()

        fields_by_name = {f.name: f for f in schema.fields}
        role_field = fields_by_name["role"]

        assert role_field.enum is not None
        assert "admin" in role_field.enum
        assert "user" in role_field.enum

    def test_parse_constraints(self, sample_schema):
        """Test that constraints are extracted."""
        parser = CanonicalSchemaParser(sample_schema)
        schema = parser.parse()

        fields_by_name = {f.name: f for f in schema.fields}

        username = fields_by_name["username"]
        assert username.min_length == 3
        assert username.max_length == 50

        age = fields_by_name["age"]
        assert age.minimum == 0
        assert age.maximum == 150

    def test_parse_nested_object(self):
        """Test parsing nested object."""
        schema_data = {
            "name": "orders",
            "fields": [
                {"name": "id", "type": "string", "format": "uuid"},
                {
                    "name": "customer",
                    "type": "object",
                    "properties": {
                        "name": {"name": "name", "type": "string"},
                        "email": {"name": "email", "type": "string", "format": "email"},
                    }
                }
            ]
        }
        parser = CanonicalSchemaParser(schema_data)
        schema = parser.parse()

        fields_by_name = {f.name: f for f in schema.fields}
        customer = fields_by_name["customer"]

        assert customer.type == "object"
        assert customer.properties is not None
        assert "name" in customer.properties
        assert "email" in customer.properties
        assert customer.properties["email"].format == "email"

    def test_parse_array_field(self):
        """Test parsing array field."""
        schema_data = {
            "name": "users",
            "fields": [
                {"name": "id", "type": "string"},
                {
                    "name": "tags",
                    "type": "array",
                    "items": {"name": "tag", "type": "string"},
                    "unique_items": True,
                    "max_items": 10
                }
            ]
        }
        parser = CanonicalSchemaParser(schema_data)
        schema = parser.parse()

        fields_by_name = {f.name: f for f in schema.fields}
        tags = fields_by_name["tags"]

        assert tags.type == "array"
        assert tags.items is not None
        assert tags.items.type == "string"
        assert tags.unique_items is True
        assert tags.max_items == 10

    def test_parse_default_value(self):
        """Test parsing default values."""
        schema_data = {
            "name": "users",
            "fields": [
                {"name": "id", "type": "string"},
                {"name": "role", "type": "string", "default": "user"},
            ]
        }
        parser = CanonicalSchemaParser(schema_data)
        schema = parser.parse()

        fields_by_name = {f.name: f for f in schema.fields}
        assert fields_by_name["role"].default == "user"


class TestCanonicalSchemaValidation:
    """Tests for canonical schema validation."""

    def test_missing_name_raises(self):
        """Test that missing name raises error."""
        schema = {"fields": [{"name": "id", "type": "string"}]}
        with pytest.raises(ValueError) as exc_info:
            CanonicalSchemaParser(schema)
        assert "name" in str(exc_info.value)

    def test_missing_fields_raises(self):
        """Test that missing fields raises error."""
        schema = {"name": "users"}
        with pytest.raises(ValueError) as exc_info:
            CanonicalSchemaParser(schema)
        assert "fields" in str(exc_info.value)

    def test_empty_fields_raises(self):
        """Test that empty fields raises error."""
        schema = {"name": "users", "fields": []}
        with pytest.raises(ValueError) as exc_info:
            CanonicalSchemaParser(schema)
        assert "empty" in str(exc_info.value)

    def test_field_missing_name_raises(self):
        """Test that field missing name raises error."""
        schema = {"name": "users", "fields": [{"type": "string"}]}
        with pytest.raises(ValueError) as exc_info:
            CanonicalSchemaParser(schema)
        assert "name" in str(exc_info.value)

    def test_field_missing_type_raises(self):
        """Test that field missing type raises error."""
        schema = {"name": "users", "fields": [{"name": "id"}]}
        with pytest.raises(ValueError) as exc_info:
            CanonicalSchemaParser(schema)
        assert "type" in str(exc_info.value)

    def test_invalid_type_raises(self):
        """Test that invalid type raises error."""
        schema = {"name": "users", "fields": [{"name": "id", "type": "invalid"}]}
        with pytest.raises(ValueError) as exc_info:
            CanonicalSchemaParser(schema)
        assert "invalid type" in str(exc_info.value).lower()

    def test_invalid_format_raises(self):
        """Test that invalid format raises error."""
        schema = {"name": "users", "fields": [{"name": "id", "type": "string", "format": "invalid"}]}
        with pytest.raises(ValueError) as exc_info:
            CanonicalSchemaParser(schema)
        assert "invalid format" in str(exc_info.value).lower()

    def test_invalid_json_raises(self):
        """Test that invalid JSON raises error."""
        with pytest.raises(ValueError) as exc_info:
            CanonicalSchemaParser.from_string("not valid json {")
        assert "Invalid JSON" in str(exc_info.value)


class TestIsCanonicalFormat:
    """Tests for is_canonical_format function."""

    def test_valid_canonical_format(self):
        """Test detection of valid canonical format."""
        content = json.dumps({
            "name": "users",
            "fields": [{"name": "id", "type": "string"}]
        })
        assert is_canonical_format(content) is True

    def test_missing_name(self):
        """Test detection fails without name."""
        content = json.dumps({
            "fields": [{"name": "id", "type": "string"}]
        })
        assert is_canonical_format(content) is False

    def test_missing_fields(self):
        """Test detection fails without fields."""
        content = json.dumps({"name": "users"})
        assert is_canonical_format(content) is False

    def test_fields_not_array(self):
        """Test detection fails if fields is not array."""
        content = json.dumps({"name": "users", "fields": {}})
        assert is_canonical_format(content) is False

    def test_invalid_json(self):
        """Test detection fails for invalid JSON."""
        assert is_canonical_format("not json") is False


# =============================================================================
# Unified SchemaParser Tests
# =============================================================================

class TestSchemaParser:
    """Tests for unified SchemaParser interface."""

    def test_from_file(self):
        """Test creating parser from file."""
        parser = SchemaParser.from_file(CANONICAL_FILE)
        assert parser is not None

    def test_parse(self):
        """Test parsing schema."""
        parser = SchemaParser.from_file(CANONICAL_FILE)
        schema = parser.parse()

        assert schema.name == "users"
        assert schema.source_format == SchemaFormat.CANONICAL

    def test_parse_schema_file_function(self):
        """Test parse_schema_file convenience function."""
        schema = parse_schema_file(CANONICAL_FILE)
        assert schema.name == "users"

    def test_unsupported_extension_raises(self):
        """Test that unsupported file extension raises error."""
        with tempfile.NamedTemporaryFile(suffix='.sql', delete=False) as f:
            f.write(b"CREATE TABLE users (id INT);")
            temp_path = f.name

        try:
            with pytest.raises(ValueError) as exc_info:
                SchemaParser.from_file(temp_path)
            assert "Unsupported file extension" in str(exc_info.value)
        finally:
            Path(temp_path).unlink()

    def test_non_canonical_json_raises(self):
        """Test that non-canonical JSON raises error."""
        with tempfile.NamedTemporaryFile(suffix='.json', delete=False) as f:
            f.write(b'{"type": "object", "properties": {}}')
            temp_path = f.name

        try:
            with pytest.raises(ValueError) as exc_info:
                SchemaParser.from_file(temp_path)
            assert "canonical import format" in str(exc_info.value)
        finally:
            Path(temp_path).unlink()


# =============================================================================
# Integration Tests
# =============================================================================

class TestSchemaToProfileIntegration:
    """Tests for schema to profile conversion integration."""

    def test_canonical_to_api_profile(self):
        """Test converting canonical schema to API profile."""
        parser = SchemaParser.from_file(CANONICAL_FILE)
        schema = parser.parse()
        profile = schema.to_profile_dict(dataset_type="api", count=100)

        # Verify profile structure
        assert "datasets" in profile
        assert len(profile["datasets"]) == 1
        assert profile["datasets"][0]["type"] == "api"
        assert profile["datasets"][0]["count"] == 100

    def test_canonical_to_streaming_profile(self):
        """Test converting canonical schema to streaming profile."""
        parser = SchemaParser.from_file(CANONICAL_FILE)
        schema = parser.parse()
        profile = schema.to_profile_dict(dataset_type="streaming", count=500)

        assert profile["datasets"][0]["type"] == "streaming"
        assert profile["datasets"][0]["count"] == 500

    def test_profile_output_config(self):
        """Test that profile has output configuration."""
        parser = SchemaParser.from_file(CANONICAL_FILE)
        schema = parser.parse()
        profile = schema.to_profile_dict(output_format="jsonl")

        assert "output" in profile
        assert profile["output"]["format"] == "jsonl"

    def test_profile_metadata(self):
        """Test that profile has metadata."""
        parser = SchemaParser.from_file(CANONICAL_FILE)
        schema = parser.parse()
        profile = schema.to_profile_dict()

        assert "metadata" in profile
        assert profile["metadata"]["source_format"] == "canonical"
        assert "field_count" in profile["metadata"]

    def test_write_profile_to_yaml(self):
        """Test writing profile to YAML file."""
        parser = SchemaParser.from_file(CANONICAL_FILE)
        schema = parser.parse()
        profile = schema.to_profile_dict()
        profile["name"] = "test_profile"

        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(profile, f)
            temp_path = f.name

        try:
            # Read back and verify
            with open(temp_path) as f:
                loaded = yaml.safe_load(f)

            assert loaded["name"] == "test_profile"
            assert "datasets" in loaded
        finally:
            Path(temp_path).unlink()


class TestSchemaFieldGeneration:
    """Tests for schema field generation compatibility."""

    def test_field_dict_has_type(self):
        """Test that field dict always has type."""
        field = FieldSchema(name="test", type="string")
        d = field.to_generator_schema()
        assert "type" in d
        assert d["type"] == "string"

    def test_field_dict_includes_format(self):
        """Test that field dict includes format when present."""
        field = FieldSchema(name="email", type="string", format="email")
        d = field.to_generator_schema()
        assert d["format"] == "email"

    def test_field_dict_includes_enum(self):
        """Test that field dict includes enum when present."""
        field = FieldSchema(name="role", type="string", enum=["a", "b", "c"])
        d = field.to_generator_schema()
        assert d["enum"] == ["a", "b", "c"]

    def test_nested_field_dict(self):
        """Test nested field to dict conversion."""
        inner = FieldSchema(name="street", type="string")
        outer = FieldSchema(
            name="address",
            type="object",
            properties={"street": inner}
        )
        d = outer.to_generator_schema()

        assert d["type"] == "object"
        assert "properties" in d
        assert "street" in d["properties"]

    def test_array_field_dict(self):
        """Test array field to dict conversion."""
        items = FieldSchema(name="item", type="string")
        array = FieldSchema(name="tags", type="array", items=items)
        d = array.to_generator_schema()

        assert d["type"] == "array"
        assert "items" in d
        assert d["items"]["type"] == "string"
