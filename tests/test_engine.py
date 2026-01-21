"""Tests for the Engine module."""

import pytest
import tempfile
from pathlib import Path

from persona_platform.personas.base import Persona, Behavior, BehaviorType
from persona_platform.personas.registry import PersonaRegistry
from persona_platform.profiles.base import Profile, DatasetConfig
from persona_platform.generators.base import DatasetType
from persona_platform.engine.persona_engine import PersonaEngine, GenerationResult
from persona_platform.engine.validation_engine import (
    ValidationEngine,
    ValidationResult,
    ValidationSeverity,
)


@pytest.fixture
def persona_registry():
    """Create a registry with test personas."""
    registry = PersonaRegistry()

    registry.register(Persona(
        name="test_persona",
        behaviors=[
            Behavior(name="timing", behavior_type=BehaviorType.TIMING, weight=0.5),
        ],
    ))

    registry.register(Persona(
        name="parent_persona",
        behaviors=[
            Behavior(name="parent_behavior", behavior_type=BehaviorType.LATENCY),
        ],
    ))

    registry.register(Persona(
        name="child_persona",
        inherits_from=["parent_persona"],
        behaviors=[
            Behavior(name="child_behavior", behavior_type=BehaviorType.ERROR_RATE),
        ],
    ))

    return registry


@pytest.fixture
def test_profile():
    """Create a test profile."""
    return Profile(
        name="test_profile",
        personas=["test_persona"],
        datasets=[
            DatasetConfig(dataset_type=DatasetType.API, count=10),
            DatasetConfig(dataset_type=DatasetType.FILE, count=5, enabled=False),
        ],
        seed=42,
    )


class TestPersonaEngine:
    """Tests for PersonaEngine."""

    def test_generate_from_profile(self, persona_registry, test_profile):
        engine = PersonaEngine(persona_registry=persona_registry)
        result = engine.generate(test_profile)

        assert isinstance(result, GenerationResult)
        assert result.total_records > 0
        assert result.profile.name == "test_profile"

    def test_generate_single(self, persona_registry):
        engine = PersonaEngine(persona_registry=persona_registry)
        dataset = engine.generate_single(
            persona_name="test_persona",
            dataset_type=DatasetType.API,
            count=5,
            seed=42,
        )

        assert dataset is not None
        assert len(dataset.records) == 5

    def test_generate_single_missing_persona(self, persona_registry):
        engine = PersonaEngine(persona_registry=persona_registry)
        dataset = engine.generate_single(
            persona_name="nonexistent",
            dataset_type=DatasetType.API,
        )

        assert dataset is None

    def test_stream_records(self, persona_registry):
        engine = PersonaEngine(persona_registry=persona_registry)
        stream = engine.stream(
            persona_name="test_persona",
            dataset_type=DatasetType.API,
            count=10,
        )

        records = list(stream)
        assert len(records) == 10

    def test_inheritance_resolved(self, persona_registry):
        engine = PersonaEngine(persona_registry=persona_registry)
        dataset = engine.generate_single(
            persona_name="child_persona",
            dataset_type=DatasetType.API,
            count=5,
        )

        assert dataset is not None

    def test_list_personas(self, persona_registry):
        engine = PersonaEngine(persona_registry=persona_registry)
        personas = engine.list_personas()

        assert "test_persona" in personas
        assert "parent_persona" in personas

    def test_list_generators(self, persona_registry):
        engine = PersonaEngine(persona_registry=persona_registry)
        generators = engine.list_generators()

        assert DatasetType.API in generators
        assert DatasetType.STREAMING in generators

    def test_export_result(self, persona_registry, test_profile):
        engine = PersonaEngine(persona_registry=persona_registry)
        result = engine.generate(test_profile)

        with tempfile.TemporaryDirectory() as tmpdir:
            files = engine.export_result(result, tmpdir, format="json")

            assert len(files) > 0
            for f in files:
                assert Path(f).exists()


class TestGenerationResult:
    """Tests for GenerationResult."""

    def test_result_summary(self, persona_registry, test_profile):
        engine = PersonaEngine(persona_registry=persona_registry)
        result = engine.generate(test_profile)

        summary = result.summary()

        assert "profile" in summary
        assert "total_records" in summary
        assert "duration_seconds" in summary
        assert "datasets" in summary


class TestValidationEngine:
    """Tests for ValidationEngine."""

    def test_validate_valid_persona(self):
        engine = ValidationEngine()
        persona = Persona(
            name="valid_persona",
            behaviors=[
                Behavior(name="b1", behavior_type=BehaviorType.TIMING, weight=0.5),
            ],
        )

        result = engine.validate_persona(persona)

        assert result.valid
        assert result.error_count == 0

    def test_validate_invalid_persona_name(self):
        engine = ValidationEngine()
        persona = Persona(name="")

        result = engine.validate_persona(persona)

        assert not result.valid
        assert result.error_count > 0

    def test_validate_duplicate_behavior_names(self):
        engine = ValidationEngine()
        persona = Persona(
            name="test",
            behaviors=[
                Behavior(name="duplicate", behavior_type=BehaviorType.TIMING),
                Behavior(name="duplicate", behavior_type=BehaviorType.LATENCY),
            ],
        )

        result = engine.validate_persona(persona)

        assert not result.valid

    def test_validate_valid_profile(self):
        engine = ValidationEngine()
        profile = Profile(
            name="valid_profile",
            personas=["p1"],
            datasets=[
                DatasetConfig(dataset_type=DatasetType.API, count=10),
            ],
        )

        result = engine.validate_profile(profile)

        assert result.valid

    def test_validate_profile_empty_personas_warning(self):
        engine = ValidationEngine()
        profile = Profile(name="test", personas=[])

        result = engine.validate_profile(profile)

        assert result.warning_count > 0

    def test_validate_data_against_schema(self):
        engine = ValidationEngine()
        schema = {
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "age": {"type": "integer"},
            },
            "required": ["name"],
        }

        valid_data = {"name": "Test", "age": 25}
        result = engine.validate_data_against_schema(valid_data, schema)
        assert result.valid

        invalid_data = {"age": "not an integer"}
        result = engine.validate_data_against_schema(invalid_data, schema)
        assert not result.valid

    def test_create_schema_from_sample(self):
        engine = ValidationEngine()
        sample = {
            "name": "Test",
            "age": 25,
            "active": True,
            "scores": [1, 2, 3],
        }

        schema = engine.create_schema_from_sample(sample)

        assert schema["type"] == "object"
        assert "name" in schema["properties"]
        assert schema["properties"]["name"]["type"] == "string"
        assert schema["properties"]["age"]["type"] == "integer"
        assert schema["properties"]["active"]["type"] == "boolean"
        assert schema["properties"]["scores"]["type"] == "array"


class TestValidationResult:
    """Tests for ValidationResult."""

    def test_add_issue(self):
        result = ValidationResult(valid=True)
        result.add_issue(ValidationSeverity.ERROR, "Test error")

        assert not result.valid
        assert result.error_count == 1

    def test_add_warning(self):
        result = ValidationResult(valid=True)
        result.add_issue(ValidationSeverity.WARNING, "Test warning")

        assert result.valid
        assert result.warning_count == 1

    def test_merge_results(self):
        result1 = ValidationResult(valid=True, validated_count=5)
        result1.add_issue(ValidationSeverity.WARNING, "Warning 1")

        result2 = ValidationResult(valid=True, validated_count=3)
        result2.add_issue(ValidationSeverity.WARNING, "Warning 2")

        merged = result1.merge(result2)

        assert merged.valid
        assert merged.validated_count == 8
        assert merged.warning_count == 2

    def test_to_dict(self):
        result = ValidationResult(valid=True, validated_count=10)
        result.add_issue(ValidationSeverity.INFO, "Info message")

        data = result.to_dict()

        assert "valid" in data
        assert "error_count" in data
        assert "issues" in data
