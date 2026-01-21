"""Tests for the Profiles module."""

import pytest
import tempfile
from pathlib import Path

from persona_platform.profiles.base import (
    Profile,
    ProfileBuilder,
    DatasetConfig,
    OutputConfig,
    OutputFormat,
)
from persona_platform.profiles.loader import ProfileLoader, load_profile
from persona_platform.generators.base import DatasetType


class TestProfile:
    """Tests for Profile class."""

    def test_create_profile(self):
        profile = Profile(
            name="test_profile",
            description="A test profile",
            personas=["persona1", "persona2"],
        )

        assert profile.name == "test_profile"
        assert len(profile.personas) == 2

    def test_add_dataset_config(self):
        profile = Profile(name="test")
        config = DatasetConfig(
            dataset_type=DatasetType.API,
            count=100,
        )

        profile.add_dataset(config)
        assert len(profile.datasets) == 1

    def test_get_enabled_datasets(self):
        profile = Profile(
            name="test",
            datasets=[
                DatasetConfig(dataset_type=DatasetType.API, enabled=True),
                DatasetConfig(dataset_type=DatasetType.FILE, enabled=False),
                DatasetConfig(dataset_type=DatasetType.STREAMING, enabled=True),
            ],
        )

        enabled = profile.get_enabled_datasets()
        assert len(enabled) == 2

    def test_get_dataset_config(self):
        profile = Profile(
            name="test",
            datasets=[
                DatasetConfig(dataset_type=DatasetType.API, count=100),
            ],
        )

        config = profile.get_dataset_config(DatasetType.API)
        assert config is not None
        assert config.count == 100

        assert profile.get_dataset_config(DatasetType.FILE) is None

    def test_add_remove_persona(self):
        profile = Profile(name="test")

        profile.add_persona("persona1")
        assert "persona1" in profile.personas

        profile.add_persona("persona1")
        assert profile.personas.count("persona1") == 1

        assert profile.remove_persona("persona1")
        assert "persona1" not in profile.personas
        assert not profile.remove_persona("nonexistent")

    def test_resolve_variables(self):
        profile = Profile(
            name="test",
            variables={"env": "prod", "version": "1.0"},
        )

        result = profile.resolve_variables("Deploy to ${env} version ${version}")
        assert result == "Deploy to prod version 1.0"


class TestProfileBuilder:
    """Tests for ProfileBuilder."""

    def test_builder_creates_profile(self):
        profile = (
            ProfileBuilder("test")
            .description("Test profile")
            .version("2.0.0")
            .persona("persona1", "persona2")
            .dataset(DatasetType.API, count=500)
            .dataset("streaming", count=1000)
            .seed(42)
            .output_format(OutputFormat.JSON)
            .output_directory("./test_output")
            .variable("key", "value")
            .tag("test", "builder")
            .build()
        )

        assert profile.name == "test"
        assert profile.version == "2.0.0"
        assert len(profile.personas) == 2
        assert len(profile.datasets) == 2
        assert profile.seed == 42
        assert profile.variables["key"] == "value"


class TestOutputConfig:
    """Tests for OutputConfig."""

    def test_default_values(self):
        config = OutputConfig()

        assert config.format == OutputFormat.JSON
        assert config.directory == "./output"
        assert config.compress is False
        assert config.pretty_print is True


class TestDatasetConfig:
    """Tests for DatasetConfig."""

    def test_create_config(self):
        config = DatasetConfig(
            dataset_type=DatasetType.API,
            count=500,
            options={"method": "POST"},
        )

        assert config.dataset_type == DatasetType.API
        assert config.count == 500
        assert config.options["method"] == "POST"
        assert config.enabled is True

    def test_count_minimum(self):
        with pytest.raises(ValueError):
            DatasetConfig(dataset_type=DatasetType.API, count=0)


class TestProfileLoader:
    """Tests for ProfileLoader."""

    def test_load_and_save_profile(self):
        original = Profile(
            name="test_profile",
            description="Test",
            personas=["p1", "p2"],
            datasets=[
                DatasetConfig(dataset_type=DatasetType.API, count=100),
            ],
            seed=42,
        )

        loader = ProfileLoader()

        with tempfile.TemporaryDirectory() as tmpdir:
            filepath = Path(tmpdir) / "test_profile.yaml"
            loader.save_file(original, filepath)

            loaded = loader.load_file(filepath)

            assert loaded.name == original.name
            assert loaded.personas == original.personas
            assert loaded.seed == original.seed
            assert len(loaded.datasets) == len(original.datasets)

    def test_load_from_string(self):
        yaml_content = """
name: test
description: Test profile
personas:
  - persona1
datasets:
  - type: api
    count: 100
seed: 123
"""
        loader = ProfileLoader()
        profile = loader.load_from_string(yaml_content)

        assert profile.name == "test"
        assert "persona1" in profile.personas
        assert profile.seed == 123


class TestLoadProfile:
    """Tests for load_profile convenience function."""

    def test_load_profile_function(self):
        yaml_content = """
name: convenience_test
personas:
  - test_persona
datasets:
  - type: streaming
    count: 500
"""
        with tempfile.TemporaryDirectory() as tmpdir:
            filepath = Path(tmpdir) / "test.yaml"
            filepath.write_text(yaml_content)

            profile = load_profile(filepath)
            assert profile.name == "convenience_test"
