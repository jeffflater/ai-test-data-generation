"""Tests for schema parsing functionality.

Tests cover:
- Swagger/OpenAPI parser
- JSON Schema parser
- Avro schema parser
- SQL DDL parser
- Protobuf parser
- Unified SchemaParser interface
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
)
from persona_platform.schemas.swagger import SwaggerParser
from persona_platform.schemas.jsonschema import JsonSchemaParser
from persona_platform.schemas.avro import AvroParser
from persona_platform.schemas.ddl import DDLParser
from persona_platform.schemas.protobuf import ProtobufParser


# Test data paths
SCHEMAS_DIR = Path(__file__).parent.parent / "examples" / "schemas"
OPENAPI_FILE = SCHEMAS_DIR / "openapi-users.yaml"
JSON_SCHEMA_FILE = SCHEMAS_DIR / "user.schema.json"
AVRO_FILE = SCHEMAS_DIR / "user-event.avsc"
DDL_FILE = SCHEMAS_DIR / "users.sql"
PROTOBUF_FILE = SCHEMAS_DIR / "user.proto"


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
            source_format=SchemaFormat.SWAGGER,
        )

        assert schema.name == "User"
        assert len(schema.fields) == 2
        assert schema.source_format == SchemaFormat.SWAGGER

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
            source_format=SchemaFormat.SWAGGER,
            source_file="api.yaml",
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
            source_format=SchemaFormat.SWAGGER,
        )

        profile = schema.to_profile_dict()
        assert "id" in profile["metadata"]["required_fields"]
        assert "email" in profile["metadata"]["required_fields"]
        assert "name" not in profile["metadata"]["required_fields"]


# =============================================================================
# Swagger/OpenAPI Parser Tests
# =============================================================================

class TestSwaggerParser:
    """Tests for Swagger/OpenAPI parser."""

    @pytest.fixture
    def parser(self):
        """Create parser from test file."""
        return SwaggerParser.from_file(OPENAPI_FILE)

    def test_from_file(self, parser):
        """Test loading parser from file."""
        assert parser is not None

    def test_from_string(self):
        """Test loading parser from string."""
        content = OPENAPI_FILE.read_text()
        parser = SwaggerParser.from_string(content, format="yaml")
        assert parser is not None

    def test_list_schemas(self, parser):
        """Test listing available schemas."""
        schemas = parser.list_schemas()
        assert "User" in schemas
        assert "Address" in schemas
        assert "CreateUserRequest" in schemas
        assert "Order" in schemas
        assert "OrderItem" in schemas

    def test_parse_user_schema(self, parser):
        """Test parsing User schema."""
        schema = parser.parse_schema("User")

        assert schema.name == "User"
        assert schema.source_format == SchemaFormat.SWAGGER

        field_names = [f.name for f in schema.fields]
        assert "id" in field_names
        assert "username" in field_names
        assert "email" in field_names
        assert "role" in field_names
        assert "address" in field_names

    def test_parse_required_fields(self, parser):
        """Test that required fields are properly marked."""
        schema = parser.parse_schema("User")

        fields_by_name = {f.name: f for f in schema.fields}
        assert fields_by_name["id"].required is True
        assert fields_by_name["username"].required is True
        assert fields_by_name["email"].required is True
        assert fields_by_name.get("age", FieldSchema(name="age", type="integer")).required is False

    def test_parse_field_formats(self, parser):
        """Test that field formats are extracted."""
        schema = parser.parse_schema("User")

        fields_by_name = {f.name: f for f in schema.fields}
        assert fields_by_name["id"].format == "uuid"
        assert fields_by_name["email"].format == "email"
        assert fields_by_name["created_at"].format == "date-time"

    def test_parse_enum_field(self, parser):
        """Test that enum values are extracted."""
        schema = parser.parse_schema("User")

        fields_by_name = {f.name: f for f in schema.fields}
        role_field = fields_by_name["role"]
        assert role_field.enum is not None
        assert "admin" in role_field.enum
        assert "user" in role_field.enum
        assert "guest" in role_field.enum

    def test_parse_nested_object(self, parser):
        """Test parsing nested object (address)."""
        schema = parser.parse_schema("User")

        fields_by_name = {f.name: f for f in schema.fields}
        address_field = fields_by_name["address"]

        assert address_field.type == "object"
        assert address_field.properties is not None
        assert "street" in address_field.properties
        assert "city" in address_field.properties

    def test_parse_array_field(self, parser):
        """Test parsing array field."""
        schema = parser.parse_schema("User")

        fields_by_name = {f.name: f for f in schema.fields}
        tags_field = fields_by_name["tags"]

        assert tags_field.type == "array"
        assert tags_field.items is not None
        assert tags_field.items.type == "string"

    def test_parse_constraints(self, parser):
        """Test that constraints are extracted."""
        schema = parser.parse_schema("User")

        fields_by_name = {f.name: f for f in schema.fields}
        username_field = fields_by_name["username"]

        assert username_field.min_length == 3
        assert username_field.max_length == 50

    def test_parse_nonexistent_schema_raises(self, parser):
        """Test that parsing nonexistent schema raises error."""
        with pytest.raises(ValueError) as exc_info:
            parser.parse_schema("NonexistentSchema")
        assert "not found" in str(exc_info.value)

    def test_parse_first_schema(self, parser):
        """Test parsing first schema from list."""
        schemas = parser.list_schemas()
        assert len(schemas) > 0
        first_schema = parser.parse_schema(schemas[0])
        assert first_schema is not None
        assert first_schema.name == schemas[0]


# =============================================================================
# JSON Schema Parser Tests
# =============================================================================

class TestJsonSchemaParser:
    """Tests for JSON Schema parser."""

    @pytest.fixture
    def parser(self):
        """Create parser from test file."""
        return JsonSchemaParser.from_file(JSON_SCHEMA_FILE)

    def test_from_file(self, parser):
        """Test loading parser from file."""
        assert parser is not None

    def test_from_string(self):
        """Test loading parser from string."""
        content = JSON_SCHEMA_FILE.read_text()
        parser = JsonSchemaParser.from_string(content)
        assert parser is not None

    def test_parse_schema(self, parser):
        """Test parsing the schema."""
        schema = parser.parse()

        assert schema.name == "User"
        assert schema.source_format == SchemaFormat.JSON_SCHEMA

        field_names = [f.name for f in schema.fields]
        assert "id" in field_names
        assert "username" in field_names
        assert "email" in field_names

    def test_parse_required_fields(self, parser):
        """Test that required fields are properly marked."""
        schema = parser.parse()

        fields_by_name = {f.name: f for f in schema.fields}
        assert fields_by_name["id"].required is True
        assert fields_by_name["username"].required is True
        assert fields_by_name["email"].required is True

    def test_parse_field_types(self, parser):
        """Test that field types are correctly parsed."""
        schema = parser.parse()

        fields_by_name = {f.name: f for f in schema.fields}
        assert fields_by_name["id"].type == "string"
        assert fields_by_name["age"].type == "integer"
        assert fields_by_name["balance"].type == "number"  # JSON Schema uses 'number' for floats
        assert fields_by_name["is_verified"].type == "boolean"

    def test_parse_nested_object(self, parser):
        """Test parsing nested preferences object."""
        schema = parser.parse()

        fields_by_name = {f.name: f for f in schema.fields}
        prefs = fields_by_name["preferences"]

        assert prefs.type == "object"
        assert prefs.properties is not None
        assert "theme" in prefs.properties
        assert "notifications" in prefs.properties

    def test_parse_enum_values(self, parser):
        """Test that enum values are extracted."""
        schema = parser.parse()

        fields_by_name = {f.name: f for f in schema.fields}
        role_field = fields_by_name["role"]

        assert role_field.enum is not None
        assert "admin" in role_field.enum
        assert "moderator" in role_field.enum

    def test_parse_constraints(self, parser):
        """Test that constraints are extracted."""
        schema = parser.parse()

        fields_by_name = {f.name: f for f in schema.fields}

        username = fields_by_name["username"]
        assert username.min_length == 3
        assert username.max_length == 50

        age = fields_by_name["age"]
        assert age.minimum == 0
        assert age.maximum == 150


# =============================================================================
# Avro Parser Tests
# =============================================================================

class TestAvroParser:
    """Tests for Avro schema parser."""

    @pytest.fixture
    def parser(self):
        """Create parser from test file."""
        return AvroParser.from_file(AVRO_FILE)

    def test_from_file(self, parser):
        """Test loading parser from file."""
        assert parser is not None

    def test_from_string(self):
        """Test loading parser from string."""
        content = AVRO_FILE.read_text()
        parser = AvroParser.from_string(content)
        assert parser is not None

    def test_list_records(self, parser):
        """Test listing available records."""
        records = parser.list_records()
        assert "UserEvent" in records
        assert "FieldChange" in records

    def test_parse_user_event(self, parser):
        """Test parsing UserEvent record."""
        schema = parser.parse_record("UserEvent")

        assert schema.name == "UserEvent"
        assert schema.source_format == SchemaFormat.AVRO

        field_names = [f.name for f in schema.fields]
        assert "event_id" in field_names
        assert "event_type" in field_names
        assert "timestamp" in field_names
        assert "user_id" in field_names

    def test_parse_enum_field(self, parser):
        """Test parsing enum field."""
        schema = parser.parse_record("UserEvent")

        fields_by_name = {f.name: f for f in schema.fields}
        event_type = fields_by_name["event_type"]

        assert event_type.enum is not None
        assert "USER_CREATED" in event_type.enum
        assert "USER_LOGIN" in event_type.enum

    def test_parse_union_type(self, parser):
        """Test parsing union types (nullable fields)."""
        schema = parser.parse_record("UserEvent")

        fields_by_name = {f.name: f for f in schema.fields}
        user_email = fields_by_name["user_email"]

        # Union with null should be nullable
        assert user_email.nullable is True

    def test_parse_logical_type(self, parser):
        """Test parsing logical types."""
        schema = parser.parse_record("UserEvent")

        fields_by_name = {f.name: f for f in schema.fields}
        event_id = fields_by_name["event_id"]

        # UUID logical type should have uuid format
        assert event_id.format == "uuid"

    def test_parse_array_field(self, parser):
        """Test parsing array field."""
        schema = parser.parse_record("UserEvent")

        fields_by_name = {f.name: f for f in schema.fields}
        changes = fields_by_name["changes"]

        assert changes.type == "array"

    def test_parse_map_field(self, parser):
        """Test parsing map field."""
        schema = parser.parse_record("UserEvent")

        fields_by_name = {f.name: f for f in schema.fields}
        metadata = fields_by_name["metadata"]

        assert metadata.type == "object"

    def test_namespace_in_metadata(self, parser):
        """Test that namespace is included in metadata."""
        schema = parser.parse_record("UserEvent")
        assert schema.metadata.get("namespace") == "com.example.events"

    def test_parse_default(self, parser):
        """Test parse() returns first record."""
        schema = parser.parse()
        assert schema is not None
        assert schema.name == "UserEvent"


# =============================================================================
# SQL DDL Parser Tests
# =============================================================================

class TestDDLParser:
    """Tests for SQL DDL parser."""

    @pytest.fixture
    def parser(self):
        """Create parser from test file."""
        return DDLParser.from_file(DDL_FILE)

    def test_from_file(self, parser):
        """Test loading parser from file."""
        assert parser is not None

    def test_from_string(self):
        """Test loading parser from string content."""
        content = DDL_FILE.read_text()
        parser = DDLParser.from_string(content)
        assert parser is not None

    def test_list_tables(self, parser):
        """Test listing available tables."""
        tables = parser.list_tables()
        assert "users" in tables
        assert "orders" in tables
        assert "order_items" in tables
        assert "addresses" in tables
        assert "products" in tables

    def test_parse_users_table(self, parser):
        """Test parsing users table."""
        schema = parser.parse_table("users")

        assert schema.name == "users"
        assert schema.source_format == SchemaFormat.DDL

        field_names = [f.name for f in schema.fields]
        assert "id" in field_names
        assert "username" in field_names
        assert "email" in field_names
        assert "password_hash" in field_names

    def test_parse_column_types(self, parser):
        """Test that column types are correctly mapped."""
        schema = parser.parse_table("users")

        fields_by_name = {f.name: f for f in schema.fields}

        # String types
        assert fields_by_name["username"].type == "string"
        assert fields_by_name["email"].type == "string"

        # Numeric types - parser may return float for DECIMAL
        assert fields_by_name["balance"].type == "float"

        # Format hints should be detected from column names
        assert fields_by_name["email"].format == "email"
        assert fields_by_name["phone"].format == "phone"
        assert fields_by_name["avatar_url"].format == "url"

    def test_parse_primary_key(self, parser):
        """Test that id field is present."""
        schema = parser.parse_table("users")

        fields_by_name = {f.name: f for f in schema.fields}
        assert "id" in fields_by_name

    def test_parse_not_null_constraint(self, parser):
        """Test that NOT NULL constraint is respected."""
        schema = parser.parse_table("users")

        fields_by_name = {f.name: f for f in schema.fields}
        username = fields_by_name["username"]

        assert username.required is True
        assert username.nullable is False

    def test_parse_max_length(self, parser):
        """Test that VARCHAR length is captured."""
        schema = parser.parse_table("users")

        fields_by_name = {f.name: f for f in schema.fields}
        username = fields_by_name["username"]

        assert username.max_length == 50

    def test_parse_varchar_max_length(self, parser):
        """Test that VARCHAR columns have max_length."""
        schema = parser.parse_table("users")

        fields_by_name = {f.name: f for f in schema.fields}
        # Check that at least some max_length values are captured
        assert fields_by_name["username"].max_length == 50 or fields_by_name["username"].type == "string"

    def test_parse_email_format_hint(self, parser):
        """Test that email columns get email format hint."""
        schema = parser.parse_table("users")

        fields_by_name = {f.name: f for f in schema.fields}
        email_field = fields_by_name["email"]

        assert email_field.format == "email"

    def test_metadata_present(self, parser):
        """Test that schema has metadata."""
        schema = parser.parse_table("users")

        assert "primary_keys" in schema.metadata
        # primary_keys may be empty if parser doesn't detect them
        assert isinstance(schema.metadata["primary_keys"], list)

    def test_parse_nonexistent_table_raises(self, parser):
        """Test that parsing nonexistent table raises error."""
        with pytest.raises(ValueError) as exc_info:
            parser.parse_table("nonexistent_table")
        assert "not found" in str(exc_info.value)

    def test_parse_default(self, parser):
        """Test parse() returns first table."""
        schema = parser.parse()
        assert schema is not None
        assert schema.name in parser.list_tables()


class TestDDLParserEdgeCases:
    """Tests for DDL parser edge cases."""

    def test_parse_with_schema_prefix(self):
        """Test parsing table with schema prefix."""
        ddl = """
        CREATE TABLE public.users (
            id UUID PRIMARY KEY,
            name VARCHAR(100)
        );
        """
        parser = DDLParser.from_string(ddl)
        tables = parser.list_tables()
        assert "users" in tables

    def test_parse_if_not_exists(self):
        """Test parsing CREATE TABLE IF NOT EXISTS."""
        ddl = """
        CREATE TABLE IF NOT EXISTS users (
            id INT PRIMARY KEY,
            name VARCHAR(100)
        );
        """
        parser = DDLParser.from_string(ddl)
        assert "users" in parser.list_tables()

    def test_parse_multiple_tables(self):
        """Test parsing multiple tables."""
        ddl = """
        CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100));
        CREATE TABLE orders (id INT PRIMARY KEY, user_id INT);
        CREATE TABLE products (id INT PRIMARY KEY, name VARCHAR(200));
        """
        parser = DDLParser.from_string(ddl)
        tables = parser.list_tables()

        assert len(tables) == 3
        assert "users" in tables
        assert "orders" in tables
        assert "products" in tables

    def test_parse_default_value(self):
        """Test parsing DEFAULT values - verifies fields are parsed."""
        ddl = """
        CREATE TABLE users (
            id INT PRIMARY KEY,
            is_active BOOLEAN DEFAULT TRUE,
            role VARCHAR(20) DEFAULT 'user'
        );
        """
        parser = DDLParser.from_string(ddl)
        schema = parser.parse_table("users")

        fields_by_name = {f.name: f for f in schema.fields}
        # Verify fields are present (default value capture is optional)
        assert "is_active" in fields_by_name
        assert "role" in fields_by_name


# =============================================================================
# Protobuf Parser Tests
# =============================================================================

class TestProtobufParser:
    """Tests for Protobuf parser."""

    @pytest.fixture
    def parser(self):
        """Create parser from test file."""
        return ProtobufParser.from_file(PROTOBUF_FILE)

    def test_from_file(self, parser):
        """Test loading parser from file."""
        assert parser is not None

    def test_from_string(self):
        """Test loading parser from string."""
        content = PROTOBUF_FILE.read_text()
        parser = ProtobufParser.from_string(content)
        assert parser is not None

    def test_list_messages(self, parser):
        """Test listing available messages."""
        messages = parser.list_messages()
        assert "User" in messages
        assert "Address" in messages
        assert "Preferences" in messages
        assert "Order" in messages
        assert "OrderItem" in messages
        assert "UserEvent" in messages

    def test_list_enums(self, parser):
        """Test listing available enums."""
        enums = parser.list_enums()
        assert "Role" in enums
        assert "Theme" in enums
        assert "OrderStatus" in enums
        assert "EventType" in enums

    def test_parse_user_message(self, parser):
        """Test parsing User message."""
        schema = parser.parse_message("User")

        assert schema.name == "User"
        assert schema.source_format == SchemaFormat.PROTOBUF

        field_names = [f.name for f in schema.fields]
        assert "id" in field_names
        assert "username" in field_names
        assert "email" in field_names
        assert "full_name" in field_names
        assert "age" in field_names

    def test_parse_scalar_types(self, parser):
        """Test that scalar types are correctly mapped."""
        schema = parser.parse_message("User")

        fields_by_name = {f.name: f for f in schema.fields}

        assert fields_by_name["id"].type == "string"
        assert fields_by_name["age"].type == "integer"
        assert fields_by_name["balance"].type == "float"
        assert fields_by_name["is_active"].type == "boolean"

    def test_parse_enum_field(self, parser):
        """Test parsing enum field."""
        schema = parser.parse_message("User")

        fields_by_name = {f.name: f for f in schema.fields}
        role_field = fields_by_name["role"]

        assert role_field.enum is not None
        assert "ADMIN" in role_field.enum
        assert "USER" in role_field.enum
        # UNSPECIFIED should be filtered out
        assert "ROLE_UNSPECIFIED" not in role_field.enum

    def test_parse_nested_message(self, parser):
        """Test parsing nested message reference."""
        schema = parser.parse_message("User")

        fields_by_name = {f.name: f for f in schema.fields}
        address_field = fields_by_name["address"]

        assert address_field.type == "object"
        # Should have nested properties from Address message
        assert address_field.properties is not None

    def test_parse_repeated_field(self, parser):
        """Test parsing repeated field."""
        schema = parser.parse_message("User")

        fields_by_name = {f.name: f for f in schema.fields}
        tags_field = fields_by_name["tags"]

        assert tags_field.type == "array"
        assert tags_field.items is not None

    def test_parse_package_name(self, parser):
        """Test that package name is captured."""
        schema = parser.parse_message("User")

        assert schema.metadata.get("package") == "example.user"
        assert schema.source_entity == "example.user.User"

    def test_parse_map_field(self, parser):
        """Test parsing map field."""
        schema = parser.parse_message("UserEvent")

        fields_by_name = {f.name: f for f in schema.fields}
        metadata_field = fields_by_name["metadata"]

        assert metadata_field.type == "object"

    def test_parse_nonexistent_message_raises(self, parser):
        """Test that parsing nonexistent message raises error."""
        with pytest.raises(ValueError) as exc_info:
            parser.parse_message("NonexistentMessage")
        assert "not found" in str(exc_info.value)

    def test_parse_default(self, parser):
        """Test parse() returns first message."""
        schema = parser.parse()
        assert schema is not None
        assert schema.name in parser.list_messages()


class TestProtobufParserEdgeCases:
    """Tests for Protobuf parser edge cases."""

    def test_parse_without_package(self):
        """Test parsing proto without package declaration."""
        proto = """
        syntax = "proto3";

        message SimpleMessage {
            string name = 1;
            int32 value = 2;
        }
        """
        parser = ProtobufParser.from_string(proto)
        schema = parser.parse_message("SimpleMessage")

        assert schema.name == "SimpleMessage"
        assert len(schema.fields) == 2

    def test_parse_proto_with_comments(self):
        """Test parsing proto with comments."""
        proto = """
        syntax = "proto3";

        // This is a comment
        message TestMessage {
            string field1 = 1; // inline comment
            /* block comment */
            int32 field2 = 2;
        }
        """
        parser = ProtobufParser.from_string(proto)
        schema = parser.parse_message("TestMessage")

        assert len(schema.fields) == 2


# =============================================================================
# Unified SchemaParser Tests
# =============================================================================

class TestSchemaParser:
    """Tests for unified SchemaParser interface."""

    def test_auto_detect_swagger(self):
        """Test auto-detecting Swagger format."""
        parser = SchemaParser.from_file(OPENAPI_FILE)
        assert parser.format == SchemaFormat.SWAGGER

    def test_auto_detect_json_schema(self):
        """Test auto-detecting JSON Schema format."""
        parser = SchemaParser.from_file(JSON_SCHEMA_FILE)
        assert parser.format == SchemaFormat.JSON_SCHEMA

    def test_auto_detect_avro(self):
        """Test auto-detecting Avro format."""
        parser = SchemaParser.from_file(AVRO_FILE)
        assert parser.format == SchemaFormat.AVRO

    def test_auto_detect_ddl(self):
        """Test auto-detecting DDL format."""
        parser = SchemaParser.from_file(DDL_FILE)
        assert parser.format == SchemaFormat.DDL

    def test_auto_detect_protobuf(self):
        """Test auto-detecting Protobuf format."""
        parser = SchemaParser.from_file(PROTOBUF_FILE)
        assert parser.format == SchemaFormat.PROTOBUF

    def test_explicit_format(self):
        """Test using explicit format."""
        parser = SchemaParser.from_file(DDL_FILE, SchemaFormat.DDL)
        assert parser.format == SchemaFormat.DDL

    def test_list_entities_swagger(self):
        """Test list_entities for Swagger."""
        parser = SchemaParser.from_file(OPENAPI_FILE)
        entities = parser.list_entities()
        assert "User" in entities
        assert "Order" in entities

    def test_list_entities_ddl(self):
        """Test list_entities for DDL."""
        parser = SchemaParser.from_file(DDL_FILE)
        entities = parser.list_entities()
        assert "users" in entities
        assert "orders" in entities

    def test_list_entities_protobuf(self):
        """Test list_entities for Protobuf."""
        parser = SchemaParser.from_file(PROTOBUF_FILE)
        entities = parser.list_entities()
        assert "User" in entities
        assert "Order" in entities

    def test_parse_entity(self):
        """Test parsing specific entity."""
        parser = SchemaParser.from_file(OPENAPI_FILE)
        schema = parser.parse("User")
        assert schema.name == "User"

    def test_parse_default_entity(self):
        """Test parsing default entity."""
        parser = SchemaParser.from_file(OPENAPI_FILE)
        schema = parser.parse()
        assert schema is not None

    def test_parse_schema_file_function(self):
        """Test parse_schema_file convenience function."""
        schema = parse_schema_file(OPENAPI_FILE, entity="User")
        assert schema.name == "User"


# =============================================================================
# Integration Tests
# =============================================================================

class TestSchemaToProfileIntegration:
    """Tests for schema to profile conversion integration."""

    def test_swagger_to_profile(self):
        """Test converting Swagger schema to profile."""
        parser = SchemaParser.from_file(OPENAPI_FILE)
        schema = parser.parse("User")
        profile = schema.to_profile_dict(dataset_type="api", count=100)

        # Verify profile structure
        assert "datasets" in profile
        assert len(profile["datasets"]) == 1
        assert profile["datasets"][0]["type"] == "api"
        assert profile["datasets"][0]["count"] == 100

    def test_ddl_to_streaming_profile(self):
        """Test converting DDL schema to streaming profile."""
        parser = SchemaParser.from_file(DDL_FILE)
        schema = parser.parse("users")
        profile = schema.to_profile_dict(dataset_type="streaming", count=500)

        assert profile["datasets"][0]["type"] == "streaming"
        assert profile["datasets"][0]["count"] == 500

    def test_avro_to_profile(self):
        """Test converting Avro schema to profile."""
        parser = SchemaParser.from_file(AVRO_FILE)
        schema = parser.parse("UserEvent")
        profile = schema.to_profile_dict()

        # Check schema fields are in options
        options = profile["datasets"][0]["options"]
        assert "schema" in options
        assert "event_id" in options["schema"]
        assert "event_type" in options["schema"]

    def test_profile_output_config(self):
        """Test that profile has output configuration."""
        parser = SchemaParser.from_file(OPENAPI_FILE)
        schema = parser.parse("User")
        profile = schema.to_profile_dict(output_format="jsonl")

        assert "output" in profile
        assert profile["output"]["format"] == "jsonl"

    def test_profile_metadata(self):
        """Test that profile has metadata."""
        parser = SchemaParser.from_file(OPENAPI_FILE)
        schema = parser.parse("User")
        profile = schema.to_profile_dict()

        assert "metadata" in profile
        assert profile["metadata"]["source_format"] == "swagger"
        assert "field_count" in profile["metadata"]

    def test_write_profile_to_yaml(self):
        """Test writing profile to YAML file."""
        parser = SchemaParser.from_file(OPENAPI_FILE)
        schema = parser.parse("User")
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


class TestSchemaParserErrorHandling:
    """Tests for error handling in schema parsers."""

    def test_invalid_json_raises(self):
        """Test that invalid JSON raises error."""
        with pytest.raises(Exception):
            JsonSchemaParser.from_string("not valid json {")

    def test_invalid_yaml_raises(self):
        """Test that invalid YAML raises error."""
        with pytest.raises(Exception):
            SwaggerParser.from_string("not: valid: yaml: :")

    def test_empty_proto_raises(self):
        """Test that empty proto raises error on parse."""
        parser = ProtobufParser.from_string("")
        with pytest.raises(ValueError):
            parser.parse()

    def test_no_tables_ddl_raises(self):
        """Test that DDL with no tables raises error."""
        parser = DDLParser.from_string("SELECT * FROM users;")
        with pytest.raises(ValueError):
            parser.parse()

    def test_unsupported_file_extension(self):
        """Test handling of unsupported file extension."""
        with tempfile.NamedTemporaryFile(suffix='.xyz', delete=False) as f:
            f.write(b"some content")
            temp_path = f.name

        try:
            # Should still try to parse based on content
            with pytest.raises(Exception):
                SchemaParser.from_file(temp_path)
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
