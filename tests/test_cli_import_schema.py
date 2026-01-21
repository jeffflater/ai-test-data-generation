"""Tests for import-schema CLI command.

Tests cover:
- Importing canonical JSON schemas to profile YAML
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
CANONICAL_FILE = SCHEMAS_DIR / "users.import.json"


@pytest.fixture
def runner():
    """Create CLI test runner."""
    return CliRunner()


@pytest.fixture
def temp_output_dir():
    """Create temporary output directory."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def sample_schema_file(temp_output_dir):
    """Create a sample canonical schema file."""
    schema = {
        "name": "products",
        "description": "Product catalog",
        "fields": [
            {"name": "id", "type": "string", "format": "uuid", "required": True},
            {"name": "name", "type": "string", "required": True, "max_length": 100},
            {"name": "price", "type": "float", "minimum": 0},
            {"name": "category", "type": "string", "enum": ["electronics", "clothing", "food"]},
            {"name": "in_stock", "type": "boolean", "default": True},
        ]
    }
    schema_file = temp_output_dir / "products.json"
    schema_file.write_text(json.dumps(schema, indent=2))
    return schema_file


# =============================================================================
# Import Schema Tests
# =============================================================================

class TestImportSchema:
    """Tests for importing schemas to profile."""

    def test_import_canonical_schema(self, runner, temp_output_dir):
        """Test importing canonical JSON schema."""
        output_file = temp_output_dir / "users-profile.yaml"

        result = runner.invoke(cli, [
            "import-schema",
            str(CANONICAL_FILE),
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

    def test_import_for_streaming(self, runner, temp_output_dir, sample_schema_file):
        """Test importing schema for streaming dataset type."""
        output_file = temp_output_dir / "streaming.yaml"

        result = runner.invoke(cli, [
            "import-schema",
            str(sample_schema_file),
            "-t", "streaming",
            "-o", str(output_file)
        ])

        assert result.exit_code == 0

        with open(output_file) as f:
            profile = yaml.safe_load(f)

        assert profile["datasets"][0]["type"] == "streaming"

    def test_import_for_load_testing(self, runner, temp_output_dir, sample_schema_file):
        """Test importing schema for load testing."""
        output_file = temp_output_dir / "load.yaml"

        result = runner.invoke(cli, [
            "import-schema",
            str(sample_schema_file),
            "-t", "load",
            "-c", "10000",
            "-o", str(output_file)
        ])

        assert result.exit_code == 0

        with open(output_file) as f:
            profile = yaml.safe_load(f)

        assert profile["datasets"][0]["type"] == "load"
        assert profile["datasets"][0]["count"] == 10000


# =============================================================================
# CLI Options Tests
# =============================================================================

class TestCLIOptions:
    """Tests for CLI option handling."""

    def test_custom_profile_name(self, runner, temp_output_dir, sample_schema_file):
        """Test using custom profile name."""
        output_file = temp_output_dir / "custom.yaml"

        result = runner.invoke(cli, [
            "import-schema",
            str(sample_schema_file),
            "-n", "my_custom_profile",
            "-o", str(output_file)
        ])

        assert result.exit_code == 0

        with open(output_file) as f:
            profile = yaml.safe_load(f)

        assert profile["name"] == "my_custom_profile"

    def test_custom_count(self, runner, temp_output_dir, sample_schema_file):
        """Test using custom record count."""
        output_file = temp_output_dir / "profile.yaml"

        result = runner.invoke(cli, [
            "import-schema",
            str(sample_schema_file),
            "-c", "500",
            "-o", str(output_file)
        ])

        assert result.exit_code == 0

        with open(output_file) as f:
            profile = yaml.safe_load(f)

        assert profile["datasets"][0]["count"] == 500

    def test_output_format_jsonl(self, runner, temp_output_dir, sample_schema_file):
        """Test setting output format to jsonl."""
        output_file = temp_output_dir / "profile.yaml"

        result = runner.invoke(cli, [
            "import-schema",
            str(sample_schema_file),
            "--output-format", "jsonl",
            "-o", str(output_file)
        ])

        assert result.exit_code == 0

        with open(output_file) as f:
            profile = yaml.safe_load(f)

        assert profile["output"]["format"] == "jsonl"

    def test_output_format_csv(self, runner, temp_output_dir, sample_schema_file):
        """Test setting output format to csv."""
        output_file = temp_output_dir / "profile.yaml"

        result = runner.invoke(cli, [
            "import-schema",
            str(sample_schema_file),
            "--output-format", "csv",
            "-o", str(output_file)
        ])

        assert result.exit_code == 0

        with open(output_file) as f:
            profile = yaml.safe_load(f)

        assert profile["output"]["format"] == "csv"

    def test_dataset_type_fault(self, runner, temp_output_dir, sample_schema_file):
        """Test setting dataset type to fault."""
        output_file = temp_output_dir / "profile.yaml"

        result = runner.invoke(cli, [
            "import-schema",
            str(sample_schema_file),
            "-t", "fault",
            "-o", str(output_file)
        ])

        assert result.exit_code == 0

        with open(output_file) as f:
            profile = yaml.safe_load(f)

        assert profile["datasets"][0]["type"] == "fault"

    def test_dataset_type_file(self, runner, temp_output_dir, sample_schema_file):
        """Test setting dataset type to file."""
        output_file = temp_output_dir / "profile.yaml"

        result = runner.invoke(cli, [
            "import-schema",
            str(sample_schema_file),
            "-t", "file",
            "-o", str(output_file)
        ])

        assert result.exit_code == 0

        with open(output_file) as f:
            profile = yaml.safe_load(f)

        assert profile["datasets"][0]["type"] == "file"


# =============================================================================
# Profile Content Tests
# =============================================================================

class TestProfileContent:
    """Tests for generated profile content."""

    def test_profile_has_header_comment(self, runner, temp_output_dir, sample_schema_file):
        """Test that profile has header comments."""
        output_file = temp_output_dir / "profile.yaml"

        result = runner.invoke(cli, [
            "import-schema",
            str(sample_schema_file),
            "-o", str(output_file)
        ])

        content = output_file.read_text()
        assert "# Auto-generated from:" in content
        assert "# Schema:" in content
        assert "# Usage:" in content

    def test_profile_has_metadata(self, runner, temp_output_dir, sample_schema_file):
        """Test that profile has metadata section."""
        output_file = temp_output_dir / "profile.yaml"

        result = runner.invoke(cli, [
            "import-schema",
            str(sample_schema_file),
            "-o", str(output_file)
        ])

        with open(output_file) as f:
            profile = yaml.safe_load(f)

        assert "metadata" in profile
        assert "source_file" in profile["metadata"]
        assert "source_format" in profile["metadata"]
        assert "field_count" in profile["metadata"]

    def test_profile_schema_has_fields(self, runner, temp_output_dir, sample_schema_file):
        """Test that profile schema contains expected fields."""
        output_file = temp_output_dir / "profile.yaml"

        result = runner.invoke(cli, [
            "import-schema",
            str(sample_schema_file),
            "-o", str(output_file)
        ])

        with open(output_file) as f:
            profile = yaml.safe_load(f)

        schema = profile["datasets"][0]["options"]["schema"]
        assert "id" in schema
        assert "name" in schema
        assert "price" in schema

    def test_profile_field_has_type(self, runner, temp_output_dir, sample_schema_file):
        """Test that profile fields have type information."""
        output_file = temp_output_dir / "profile.yaml"

        result = runner.invoke(cli, [
            "import-schema",
            str(sample_schema_file),
            "-o", str(output_file)
        ])

        with open(output_file) as f:
            profile = yaml.safe_load(f)

        schema = profile["datasets"][0]["options"]["schema"]
        assert schema["id"]["type"] == "string"
        assert schema["id"]["format"] == "uuid"
        assert schema["price"]["type"] == "float"

    def test_profile_tracks_required_fields(self, runner, temp_output_dir, sample_schema_file):
        """Test that profile tracks required fields."""
        output_file = temp_output_dir / "profile.yaml"

        result = runner.invoke(cli, [
            "import-schema",
            str(sample_schema_file),
            "-o", str(output_file)
        ])

        with open(output_file) as f:
            profile = yaml.safe_load(f)

        required = profile["metadata"]["required_fields"]
        assert "id" in required
        assert "name" in required


# =============================================================================
# Error Handling Tests
# =============================================================================

class TestErrorHandling:
    """Tests for error handling."""

    def test_missing_output_raises(self, runner):
        """Test that missing --output raises error."""
        result = runner.invoke(cli, [
            "import-schema",
            str(CANONICAL_FILE)
        ])

        assert result.exit_code != 0
        assert "missing" in result.output.lower() or "required" in result.output.lower()

    def test_nonexistent_file(self, runner, temp_output_dir):
        """Test error for nonexistent input file."""
        result = runner.invoke(cli, [
            "import-schema",
            "/nonexistent/path/file.json",
            "-o", str(temp_output_dir / "out.yaml")
        ])

        assert result.exit_code != 0

    def test_invalid_json_file(self, runner, temp_output_dir):
        """Test error for invalid JSON file."""
        invalid_file = temp_output_dir / "invalid.json"
        invalid_file.write_text("not valid json {")

        result = runner.invoke(cli, [
            "import-schema",
            str(invalid_file),
            "-o", str(temp_output_dir / "out.yaml")
        ])

        assert result.exit_code != 0
        assert "error" in result.output.lower()

    def test_non_canonical_json(self, runner, temp_output_dir):
        """Test error for JSON that isn't canonical format."""
        non_canonical = temp_output_dir / "other.json"
        non_canonical.write_text('{"type": "object", "properties": {}}')

        result = runner.invoke(cli, [
            "import-schema",
            str(non_canonical),
            "-o", str(temp_output_dir / "out.yaml")
        ])

        assert result.exit_code != 0
        assert "canonical" in result.output.lower()

    def test_unsupported_extension(self, runner, temp_output_dir):
        """Test error for unsupported file extension."""
        sql_file = temp_output_dir / "schema.sql"
        sql_file.write_text("CREATE TABLE users (id INT);")

        result = runner.invoke(cli, [
            "import-schema",
            str(sql_file),
            "-o", str(temp_output_dir / "out.yaml")
        ])

        assert result.exit_code != 0
        assert "unsupported" in result.output.lower()

    def test_invalid_dataset_type_choice(self, runner, temp_output_dir, sample_schema_file):
        """Test error for invalid dataset type choice."""
        result = runner.invoke(cli, [
            "import-schema",
            str(sample_schema_file),
            "-t", "invalid_type",
            "-o", str(temp_output_dir / "out.yaml")
        ])

        assert result.exit_code != 0


# =============================================================================
# End-to-End Workflow Tests
# =============================================================================

class TestEndToEndWorkflow:
    """Tests for complete import-schema -> generate workflow."""

    @pytest.fixture
    def personas_dir(self):
        """Get personas directory."""
        return Path(__file__).parent.parent / "examples" / "personas"

    def test_import_and_generate_dry_run(self, runner, temp_output_dir, personas_dir):
        """Test complete workflow: import -> generate (dry run)."""
        profile_file = temp_output_dir / "profile.yaml"

        # Import schema
        result = runner.invoke(cli, [
            "import-schema",
            str(CANONICAL_FILE),
            "-c", "10",
            "-o", str(profile_file)
        ])
        assert result.exit_code == 0

        # Generate data (dry run)
        result = runner.invoke(cli, [
            "generate",
            str(profile_file),
            "-p", str(personas_dir),
            "--persona", "normal_user",
            "--dry-run"
        ])
        assert result.exit_code == 0
        assert "normal_user" in result.output

    def test_import_and_generate_data(self, runner, temp_output_dir, personas_dir):
        """Test complete workflow: import -> generate."""
        profile_file = temp_output_dir / "profile.yaml"

        # Import schema
        result = runner.invoke(cli, [
            "import-schema",
            str(CANONICAL_FILE),
            "-c", "5",
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

    def test_streaming_workflow(self, runner, temp_output_dir, personas_dir, sample_schema_file):
        """Test workflow for streaming data generation."""
        profile_file = temp_output_dir / "streaming-profile.yaml"

        # Import schema for streaming
        result = runner.invoke(cli, [
            "import-schema",
            str(sample_schema_file),
            "-t", "streaming",
            "-c", "10",
            "-o", str(profile_file)
        ])
        assert result.exit_code == 0

        # Generate streaming data
        result = runner.invoke(cli, [
            "generate",
            str(profile_file),
            "-p", str(personas_dir),
            "--persona", "noisy_device",
            "-o", str(temp_output_dir)
        ])
        assert result.exit_code == 0


# =============================================================================
# Verbose Output Tests
# =============================================================================

class TestVerboseOutput:
    """Tests for verbose output mode."""

    def test_verbose_shows_fields(self, runner, temp_output_dir, sample_schema_file):
        """Test that verbose mode shows field details."""
        output_file = temp_output_dir / "profile.yaml"

        result = runner.invoke(cli, [
            "-v",
            "import-schema",
            str(sample_schema_file),
            "-o", str(output_file)
        ])

        assert result.exit_code == 0
        # Verbose output should show field names
        assert "id:" in result.output or "Fields:" in result.output
