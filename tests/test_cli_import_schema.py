"""Tests for import-schema CLI command.

Tests cover:
- Listing entities from various schema formats
- Importing schemas to profile YAML
- CLI option handling
- Error cases
"""

import json
import tempfile
from pathlib import Path

import pytest
import yaml
from click.testing import CliRunner

from persona_platform.cli.main import cli


# Test data paths
SCHEMAS_DIR = Path(__file__).parent.parent / "examples" / "schemas"
OPENAPI_FILE = SCHEMAS_DIR / "openapi-users.yaml"
JSON_SCHEMA_FILE = SCHEMAS_DIR / "user.schema.json"
AVRO_FILE = SCHEMAS_DIR / "user-event.avsc"
DDL_FILE = SCHEMAS_DIR / "users.sql"
PROTOBUF_FILE = SCHEMAS_DIR / "user.proto"


@pytest.fixture
def runner():
    """Create CLI test runner."""
    return CliRunner()


@pytest.fixture
def temp_output_dir():
    """Create temporary output directory."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


# =============================================================================
# List Entities Tests
# =============================================================================

class TestListEntities:
    """Tests for --list-entities option."""

    def test_list_swagger_entities(self, runner):
        """Test listing entities from Swagger/OpenAPI file."""
        result = runner.invoke(cli, [
            "import-schema",
            str(OPENAPI_FILE),
            "--list-entities"
        ])

        assert result.exit_code == 0
        assert "User" in result.output
        assert "Address" in result.output
        assert "Order" in result.output
        assert "swagger" in result.output.lower()

    def test_list_json_schema_entities(self, runner):
        """Test listing entities from JSON Schema file."""
        result = runner.invoke(cli, [
            "import-schema",
            str(JSON_SCHEMA_FILE),
            "--list-entities"
        ])

        assert result.exit_code == 0
        assert "User" in result.output

    def test_list_avro_entities(self, runner):
        """Test listing entities from Avro file."""
        result = runner.invoke(cli, [
            "import-schema",
            str(AVRO_FILE),
            "--list-entities"
        ])

        assert result.exit_code == 0
        assert "UserEvent" in result.output
        assert "avro" in result.output.lower()

    def test_list_ddl_entities(self, runner):
        """Test listing entities from SQL DDL file."""
        result = runner.invoke(cli, [
            "import-schema",
            str(DDL_FILE),
            "-f", "ddl",
            "--list-entities"
        ])

        assert result.exit_code == 0
        assert "users" in result.output
        assert "orders" in result.output
        assert "ddl" in result.output.lower()

    def test_list_protobuf_entities(self, runner):
        """Test listing entities from Protobuf file."""
        result = runner.invoke(cli, [
            "import-schema",
            str(PROTOBUF_FILE),
            "-f", "protobuf",
            "--list-entities"
        ])

        assert result.exit_code == 0
        assert "User" in result.output
        assert "Order" in result.output
        assert "UserEvent" in result.output
        assert "protobuf" in result.output.lower()


# =============================================================================
# Import Schema Tests
# =============================================================================

class TestImportSchema:
    """Tests for importing schemas to profile."""

    def test_import_swagger_user(self, runner, temp_output_dir):
        """Test importing User schema from Swagger."""
        output_file = temp_output_dir / "user-profile.yaml"

        result = runner.invoke(cli, [
            "import-schema",
            str(OPENAPI_FILE),
            "-e", "User",
            "-o", str(output_file)
        ])

        assert result.exit_code == 0
        assert "Successfully imported schema" in result.output
        assert output_file.exists()

        # Verify profile content
        with open(output_file) as f:
            profile = yaml.safe_load(f)

        assert "datasets" in profile
        assert profile["datasets"][0]["type"] == "api"
        assert "schema" in profile["datasets"][0]["options"]

    def test_import_ddl_users_table(self, runner, temp_output_dir):
        """Test importing users table from DDL."""
        output_file = temp_output_dir / "users-db.yaml"

        result = runner.invoke(cli, [
            "import-schema",
            str(DDL_FILE),
            "-f", "ddl",
            "-e", "users",
            "-o", str(output_file)
        ])

        assert result.exit_code == 0
        assert output_file.exists()

        with open(output_file) as f:
            profile = yaml.safe_load(f)

        assert profile["datasets"][0]["options"]["schema"] is not None

    def test_import_avro_for_streaming(self, runner, temp_output_dir):
        """Test importing Avro schema for streaming."""
        output_file = temp_output_dir / "events.yaml"

        result = runner.invoke(cli, [
            "import-schema",
            str(AVRO_FILE),
            "-t", "streaming",
            "-o", str(output_file)
        ])

        assert result.exit_code == 0

        with open(output_file) as f:
            profile = yaml.safe_load(f)

        assert profile["datasets"][0]["type"] == "streaming"

    def test_import_protobuf_message(self, runner, temp_output_dir):
        """Test importing Protobuf message."""
        output_file = temp_output_dir / "proto-profile.yaml"

        result = runner.invoke(cli, [
            "import-schema",
            str(PROTOBUF_FILE),
            "-f", "protobuf",
            "-e", "User",
            "-o", str(output_file)
        ])

        assert result.exit_code == 0
        assert output_file.exists()

    def test_import_json_schema(self, runner, temp_output_dir):
        """Test importing JSON Schema."""
        output_file = temp_output_dir / "json-profile.yaml"

        result = runner.invoke(cli, [
            "import-schema",
            str(JSON_SCHEMA_FILE),
            "-o", str(output_file)
        ])

        assert result.exit_code == 0
        assert output_file.exists()


# =============================================================================
# CLI Options Tests
# =============================================================================

class TestCLIOptions:
    """Tests for CLI option handling."""

    def test_custom_profile_name(self, runner, temp_output_dir):
        """Test using custom profile name."""
        output_file = temp_output_dir / "custom.yaml"

        result = runner.invoke(cli, [
            "import-schema",
            str(OPENAPI_FILE),
            "-e", "User",
            "-n", "my_custom_profile",
            "-o", str(output_file)
        ])

        assert result.exit_code == 0

        with open(output_file) as f:
            profile = yaml.safe_load(f)

        assert profile["name"] == "my_custom_profile"

    def test_custom_count(self, runner, temp_output_dir):
        """Test using custom record count."""
        output_file = temp_output_dir / "profile.yaml"

        result = runner.invoke(cli, [
            "import-schema",
            str(OPENAPI_FILE),
            "-e", "User",
            "-c", "500",
            "-o", str(output_file)
        ])

        assert result.exit_code == 0

        with open(output_file) as f:
            profile = yaml.safe_load(f)

        assert profile["datasets"][0]["count"] == 500

    def test_output_format_jsonl(self, runner, temp_output_dir):
        """Test setting output format to jsonl."""
        output_file = temp_output_dir / "profile.yaml"

        result = runner.invoke(cli, [
            "import-schema",
            str(OPENAPI_FILE),
            "-e", "User",
            "--output-format", "jsonl",
            "-o", str(output_file)
        ])

        assert result.exit_code == 0

        with open(output_file) as f:
            profile = yaml.safe_load(f)

        assert profile["output"]["format"] == "jsonl"

    def test_dataset_type_load(self, runner, temp_output_dir):
        """Test setting dataset type to load."""
        output_file = temp_output_dir / "profile.yaml"

        result = runner.invoke(cli, [
            "import-schema",
            str(OPENAPI_FILE),
            "-e", "User",
            "-t", "load",
            "-o", str(output_file)
        ])

        assert result.exit_code == 0

        with open(output_file) as f:
            profile = yaml.safe_load(f)

        assert profile["datasets"][0]["type"] == "load"

    def test_dataset_type_fault(self, runner, temp_output_dir):
        """Test setting dataset type to fault."""
        output_file = temp_output_dir / "profile.yaml"

        result = runner.invoke(cli, [
            "import-schema",
            str(OPENAPI_FILE),
            "-e", "User",
            "-t", "fault",
            "-o", str(output_file)
        ])

        assert result.exit_code == 0

        with open(output_file) as f:
            profile = yaml.safe_load(f)

        assert profile["datasets"][0]["type"] == "fault"


# =============================================================================
# Profile Content Tests
# =============================================================================

class TestProfileContent:
    """Tests for generated profile content."""

    def test_profile_has_header_comment(self, runner, temp_output_dir):
        """Test that profile has header comments."""
        output_file = temp_output_dir / "profile.yaml"

        result = runner.invoke(cli, [
            "import-schema",
            str(OPENAPI_FILE),
            "-e", "User",
            "-o", str(output_file)
        ])

        content = output_file.read_text()
        assert "# Auto-generated from:" in content
        assert "# Schema format:" in content
        assert "# Usage:" in content

    def test_profile_has_metadata(self, runner, temp_output_dir):
        """Test that profile has metadata section."""
        output_file = temp_output_dir / "profile.yaml"

        result = runner.invoke(cli, [
            "import-schema",
            str(OPENAPI_FILE),
            "-e", "User",
            "-o", str(output_file)
        ])

        with open(output_file) as f:
            profile = yaml.safe_load(f)

        assert "metadata" in profile
        assert "source_file" in profile["metadata"]
        assert "source_format" in profile["metadata"]
        assert "field_count" in profile["metadata"]

    def test_profile_schema_has_fields(self, runner, temp_output_dir):
        """Test that profile schema contains expected fields."""
        output_file = temp_output_dir / "profile.yaml"

        result = runner.invoke(cli, [
            "import-schema",
            str(OPENAPI_FILE),
            "-e", "User",
            "-o", str(output_file)
        ])

        with open(output_file) as f:
            profile = yaml.safe_load(f)

        schema = profile["datasets"][0]["options"]["schema"]
        assert "id" in schema
        assert "username" in schema
        assert "email" in schema

    def test_profile_field_has_type(self, runner, temp_output_dir):
        """Test that profile fields have type information."""
        output_file = temp_output_dir / "profile.yaml"

        result = runner.invoke(cli, [
            "import-schema",
            str(OPENAPI_FILE),
            "-e", "User",
            "-o", str(output_file)
        ])

        with open(output_file) as f:
            profile = yaml.safe_load(f)

        schema = profile["datasets"][0]["options"]["schema"]
        assert schema["id"]["type"] == "string"
        assert schema["email"]["format"] == "email"

    def test_profile_tracks_required_fields(self, runner, temp_output_dir):
        """Test that profile tracks required fields."""
        output_file = temp_output_dir / "profile.yaml"

        result = runner.invoke(cli, [
            "import-schema",
            str(OPENAPI_FILE),
            "-e", "User",
            "-o", str(output_file)
        ])

        with open(output_file) as f:
            profile = yaml.safe_load(f)

        required = profile["metadata"]["required_fields"]
        assert "id" in required
        assert "username" in required
        assert "email" in required


# =============================================================================
# Error Handling Tests
# =============================================================================

class TestErrorHandling:
    """Tests for error handling."""

    def test_missing_output_without_list_entities(self, runner):
        """Test that missing --output raises error when not listing."""
        result = runner.invoke(cli, [
            "import-schema",
            str(OPENAPI_FILE),
            "-e", "User"
        ])

        assert result.exit_code != 0
        assert "output" in result.output.lower() or "required" in result.output.lower()

    def test_nonexistent_file(self, runner, temp_output_dir):
        """Test error for nonexistent input file."""
        result = runner.invoke(cli, [
            "import-schema",
            "/nonexistent/path/file.yaml",
            "-o", str(temp_output_dir / "out.yaml")
        ])

        assert result.exit_code != 0

    def test_nonexistent_entity(self, runner, temp_output_dir):
        """Test error for nonexistent entity."""
        result = runner.invoke(cli, [
            "import-schema",
            str(OPENAPI_FILE),
            "-e", "NonexistentEntity",
            "-o", str(temp_output_dir / "out.yaml")
        ])

        assert result.exit_code != 0
        assert "not found" in result.output.lower() or "error" in result.output.lower()

    def test_invalid_format_choice(self, runner, temp_output_dir):
        """Test error for invalid format choice."""
        result = runner.invoke(cli, [
            "import-schema",
            str(OPENAPI_FILE),
            "-f", "invalid_format",
            "-o", str(temp_output_dir / "out.yaml")
        ])

        assert result.exit_code != 0

    def test_invalid_dataset_type_choice(self, runner, temp_output_dir):
        """Test error for invalid dataset type choice."""
        result = runner.invoke(cli, [
            "import-schema",
            str(OPENAPI_FILE),
            "-e", "User",
            "-t", "invalid_type",
            "-o", str(temp_output_dir / "out.yaml")
        ])

        assert result.exit_code != 0


# =============================================================================
# Generate Command with --persona Tests
# =============================================================================

class TestGenerateWithPersona:
    """Tests for generate command with --persona option."""

    @pytest.fixture
    def personas_dir(self):
        """Get personas directory."""
        return Path(__file__).parent.parent / "examples" / "personas"

    def test_generate_dry_run_with_persona(self, runner, temp_output_dir, personas_dir):
        """Test generate --dry-run with --persona option."""
        # First create a profile
        profile_file = temp_output_dir / "profile.yaml"
        runner.invoke(cli, [
            "import-schema",
            str(OPENAPI_FILE),
            "-e", "User",
            "-o", str(profile_file)
        ])

        # Then test generate with --persona
        result = runner.invoke(cli, [
            "generate",
            str(profile_file),
            "-p", str(personas_dir),
            "--persona", "normal_user",
            "--dry-run"
        ])

        assert result.exit_code == 0
        assert "normal_user" in result.output

    def test_generate_with_multiple_personas(self, runner, temp_output_dir, personas_dir):
        """Test generate with multiple --persona options."""
        profile_file = temp_output_dir / "profile.yaml"
        runner.invoke(cli, [
            "import-schema",
            str(OPENAPI_FILE),
            "-e", "User",
            "-o", str(profile_file)
        ])

        result = runner.invoke(cli, [
            "generate",
            str(profile_file),
            "-p", str(personas_dir),
            "--persona", "normal_user",
            "--persona", "edge_case_user",
            "--dry-run"
        ])

        assert result.exit_code == 0
        # Both personas should be shown
        assert "normal_user" in result.output or "edge_case_user" in result.output


# =============================================================================
# End-to-End Workflow Tests
# =============================================================================

class TestEndToEndWorkflow:
    """Tests for complete import-schema -> generate workflow."""

    @pytest.fixture
    def personas_dir(self):
        """Get personas directory."""
        return Path(__file__).parent.parent / "examples" / "personas"

    def test_swagger_to_api_generation(self, runner, temp_output_dir, personas_dir):
        """Test complete workflow: Swagger -> Profile -> API data."""
        profile_file = temp_output_dir / "api-profile.yaml"

        # Import schema
        result = runner.invoke(cli, [
            "import-schema",
            str(OPENAPI_FILE),
            "-e", "User",
            "-c", "10",
            "-o", str(profile_file)
        ])
        assert result.exit_code == 0

        # Generate data
        result = runner.invoke(cli, [
            "generate",
            str(profile_file),
            "-p", str(personas_dir),
            "--persona", "normal_user",
            "-o", str(temp_output_dir)
        ])
        assert result.exit_code == 0
        assert "Generated" in result.output

        # Check output files were created
        output_files = list(temp_output_dir.glob("*.json"))
        assert len(output_files) > 0

    def test_ddl_to_streaming_generation(self, runner, temp_output_dir, personas_dir):
        """Test workflow: DDL -> Profile -> Streaming data."""
        profile_file = temp_output_dir / "streaming-profile.yaml"

        # Import schema
        result = runner.invoke(cli, [
            "import-schema",
            str(DDL_FILE),
            "-f", "ddl",
            "-e", "users",
            "-t", "streaming",
            "-c", "20",
            "-o", str(profile_file)
        ])
        assert result.exit_code == 0

        # Generate data
        result = runner.invoke(cli, [
            "generate",
            str(profile_file),
            "-p", str(personas_dir),
            "--persona", "noisy_device",
            "-o", str(temp_output_dir)
        ])
        assert result.exit_code == 0

    def test_avro_to_file_generation(self, runner, temp_output_dir, personas_dir):
        """Test workflow: Avro -> Profile -> File data."""
        profile_file = temp_output_dir / "file-profile.yaml"

        # Import schema
        result = runner.invoke(cli, [
            "import-schema",
            str(AVRO_FILE),
            "-t", "file",
            "-c", "15",
            "-o", str(profile_file)
        ])
        assert result.exit_code == 0

        # Generate data
        result = runner.invoke(cli, [
            "generate",
            str(profile_file),
            "-p", str(personas_dir),
            "--persona", "legacy_client",
            "-o", str(temp_output_dir)
        ])
        assert result.exit_code == 0

    def test_protobuf_to_load_generation(self, runner, temp_output_dir, personas_dir):
        """Test workflow: Protobuf -> Profile -> Load test data."""
        profile_file = temp_output_dir / "load-profile.yaml"

        # Import schema
        result = runner.invoke(cli, [
            "import-schema",
            str(PROTOBUF_FILE),
            "-f", "protobuf",
            "-e", "User",
            "-t", "load",
            "-c", "100",
            "-o", str(profile_file)
        ])
        assert result.exit_code == 0

        # Generate data
        result = runner.invoke(cli, [
            "generate",
            str(profile_file),
            "-p", str(personas_dir),
            "--persona", "high_volume_system",
            "-o", str(temp_output_dir)
        ])
        assert result.exit_code == 0

    def test_schema_to_fault_generation(self, runner, temp_output_dir, personas_dir):
        """Test workflow: Schema -> Profile -> Fault injection data."""
        profile_file = temp_output_dir / "fault-profile.yaml"

        # Import schema
        result = runner.invoke(cli, [
            "import-schema",
            str(JSON_SCHEMA_FILE),
            "-t", "fault",
            "-c", "10",
            "-o", str(profile_file)
        ])
        assert result.exit_code == 0

        # Generate fault data
        result = runner.invoke(cli, [
            "generate",
            str(profile_file),
            "-p", str(personas_dir),
            "--persona", "edge_case_user",
            "-o", str(temp_output_dir)
        ])
        assert result.exit_code == 0
