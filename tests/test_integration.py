"""Integration tests for all personas × all generators.

These tests verify that every persona can be used with every generator type
to produce valid output without errors.
"""

import pytest
from pathlib import Path

from persona_platform.personas.base import Persona
from persona_platform.personas.registry import PersonaRegistry
from persona_platform.personas.loader import load_personas
from persona_platform.generators.base import DatasetType, GeneratedDataset
from persona_platform.generators.registry import GeneratorRegistry
from persona_platform.engine.persona_engine import PersonaEngine


EXAMPLES_PERSONAS_DIR = Path(__file__).parent.parent / "examples" / "personas"

# Non-fault generators (fault generator has special behavior with baseline records)
NON_FAULT_GENERATOR_TYPES = [
    DatasetType.API,
    DatasetType.STREAMING,
    DatasetType.FILE,
    DatasetType.LOAD,
]

GENERATOR_TYPES = NON_FAULT_GENERATOR_TYPES + [DatasetType.FAULT]

EXPECTED_PERSONAS = [
    "noisy_device",
    "legacy_client",
    "normal_user",
    "high_volume_system",
    "edge_case_user",
]


@pytest.fixture(scope="module")
def persona_registry():
    """Load all example personas into a registry."""
    registry = PersonaRegistry()
    load_personas(EXAMPLES_PERSONAS_DIR, registry)
    return registry


@pytest.fixture(scope="module")
def generator_registry():
    """Get the generator registry with all default generators."""
    return GeneratorRegistry()


@pytest.fixture
def fresh_persona_engine(persona_registry):
    """Create a fresh persona engine for each test (for determinism tests)."""
    return PersonaEngine(persona_registry=persona_registry)


@pytest.fixture(scope="module")
def persona_engine(persona_registry):
    """Create a persona engine with loaded personas."""
    return PersonaEngine(persona_registry=persona_registry)


class TestPersonaLoading:
    """Tests for loading all example personas."""

    def test_personas_directory_exists(self):
        assert EXAMPLES_PERSONAS_DIR.exists(), f"Personas directory not found: {EXAMPLES_PERSONAS_DIR}"
        assert EXAMPLES_PERSONAS_DIR.is_dir()

    def test_all_expected_personas_loaded(self, persona_registry):
        loaded_names = [p.name for p in persona_registry]
        for expected in EXPECTED_PERSONAS:
            assert expected in loaded_names, f"Expected persona '{expected}' not found"

    def test_persona_count(self, persona_registry):
        personas = list(persona_registry)
        assert len(personas) >= len(EXPECTED_PERSONAS)

    @pytest.mark.parametrize("persona_name", EXPECTED_PERSONAS)
    def test_persona_has_behaviors(self, persona_registry, persona_name):
        persona = persona_registry.get(persona_name)
        assert persona is not None, f"Persona '{persona_name}' not in registry"
        assert len(persona.behaviors) > 0, f"Persona '{persona_name}' has no behaviors"

    @pytest.mark.parametrize("persona_name", EXPECTED_PERSONAS)
    def test_persona_has_valid_metadata(self, persona_registry, persona_name):
        persona = persona_registry.get(persona_name)
        assert persona.name == persona_name
        assert persona.version is not None
        assert persona.description is not None


class TestGeneratorRegistry:
    """Tests for generator registry coverage."""

    def test_all_generator_types_registered(self, generator_registry):
        for dtype in GENERATOR_TYPES:
            assert dtype in generator_registry, f"Generator for {dtype} not registered"

    @pytest.mark.parametrize("dataset_type", GENERATOR_TYPES)
    def test_generator_creation(self, generator_registry, dataset_type):
        generator = generator_registry.create(dataset_type, seed=42)
        assert generator is not None, f"Failed to create generator for {dataset_type}"


class TestPersonaGeneratorMatrix:
    """Integration tests for all persona × generator combinations."""

    @pytest.mark.parametrize("persona_name", EXPECTED_PERSONAS)
    @pytest.mark.parametrize("dataset_type", NON_FAULT_GENERATOR_TYPES)
    def test_generate_dataset(self, persona_engine, persona_name, dataset_type):
        """Test that each persona can generate data with each non-fault generator type."""
        dataset = persona_engine.generate_single(
            persona_name=persona_name,
            dataset_type=dataset_type,
            count=10,
            seed=42,
        )

        assert dataset is not None, f"Failed to generate {dataset_type} for {persona_name}"
        assert isinstance(dataset, GeneratedDataset)
        assert len(dataset.records) == 10
        assert dataset.persona_name == persona_name
        assert dataset.dataset_type == dataset_type

    @pytest.mark.parametrize("persona_name", EXPECTED_PERSONAS)
    def test_generate_fault_dataset(self, persona_engine, persona_name):
        """Test fault generator (adds baseline record by default)."""
        dataset = persona_engine.generate_single(
            persona_name=persona_name,
            dataset_type=DatasetType.FAULT,
            count=10,
            seed=42,
        )

        assert dataset is not None, f"Failed to generate fault data for {persona_name}"
        assert isinstance(dataset, GeneratedDataset)
        # Fault generator adds 1 baseline + count faulty records by default
        assert len(dataset.records) == 11
        assert dataset.persona_name == persona_name
        assert dataset.dataset_type == DatasetType.FAULT

    @pytest.mark.parametrize("persona_name", EXPECTED_PERSONAS)
    def test_generate_fault_dataset_no_baseline(self, persona_engine, persona_name):
        """Test fault generator without baseline record."""
        dataset = persona_engine.generate_single(
            persona_name=persona_name,
            dataset_type=DatasetType.FAULT,
            count=10,
            seed=42,
            include_valid_baseline=False,
        )

        assert dataset is not None
        assert len(dataset.records) == 10

    @pytest.mark.parametrize("persona_name", EXPECTED_PERSONAS)
    @pytest.mark.parametrize("dataset_type", GENERATOR_TYPES)
    def test_records_have_data(self, persona_engine, persona_name, dataset_type):
        """Test that generated records contain actual data."""
        dataset = persona_engine.generate_single(
            persona_name=persona_name,
            dataset_type=dataset_type,
            count=5,
            seed=42,
        )

        for record in dataset.records:
            assert record.data is not None
            assert isinstance(record.data, dict)
            assert len(record.data) > 0

    @pytest.mark.parametrize("persona_name", EXPECTED_PERSONAS)
    @pytest.mark.parametrize("dataset_type", NON_FAULT_GENERATOR_TYPES)
    def test_seeded_generation_produces_consistent_record_count(self, persona_engine, persona_name, dataset_type):
        """Test that same seed produces consistent record counts."""
        dataset1 = persona_engine.generate_single(
            persona_name=persona_name,
            dataset_type=dataset_type,
            count=5,
            seed=12345,
        )

        dataset2 = persona_engine.generate_single(
            persona_name=persona_name,
            dataset_type=dataset_type,
            count=5,
            seed=12345,
        )

        assert len(dataset1.records) == len(dataset2.records)
        # Note: Full determinism isn't guaranteed due to timestamps/UUIDs in the generators
        # The existing test_generators.py::TestAPIGenerator::test_deterministic_with_seed
        # tests determinism for specific fields like X-Request-ID


class TestAPIGeneratorIntegration:
    """Specific integration tests for API generator."""

    @pytest.mark.parametrize("persona_name", EXPECTED_PERSONAS)
    def test_api_records_structure(self, persona_engine, persona_name):
        dataset = persona_engine.generate_single(
            persona_name=persona_name,
            dataset_type=DatasetType.API,
            count=5,
            seed=42,
        )

        for record in dataset.records:
            assert "request" in record.data
            assert "method" in record.data["request"]
            assert "endpoint" in record.data["request"]


class TestStreamingGeneratorIntegration:
    """Specific integration tests for Streaming generator."""

    @pytest.mark.parametrize("persona_name", EXPECTED_PERSONAS)
    def test_streaming_records_structure(self, persona_engine, persona_name):
        dataset = persona_engine.generate_single(
            persona_name=persona_name,
            dataset_type=DatasetType.STREAMING,
            count=5,
            seed=42,
        )

        for record in dataset.records:
            assert "topic" in record.data or "event_type" in record.data or "payload" in record.data


class TestFileGeneratorIntegration:
    """Specific integration tests for File generator."""

    @pytest.mark.parametrize("persona_name", EXPECTED_PERSONAS)
    def test_file_records_structure(self, persona_engine, persona_name):
        dataset = persona_engine.generate_single(
            persona_name=persona_name,
            dataset_type=DatasetType.FILE,
            count=5,
            seed=42,
        )

        for record in dataset.records:
            assert "content" in record.data or "file_path" in record.data or "data" in record.data


class TestLoadGeneratorIntegration:
    """Specific integration tests for Load generator."""

    @pytest.mark.parametrize("persona_name", EXPECTED_PERSONAS)
    def test_load_records_structure(self, persona_engine, persona_name):
        dataset = persona_engine.generate_single(
            persona_name=persona_name,
            dataset_type=DatasetType.LOAD,
            count=5,
            seed=42,
        )

        for record in dataset.records:
            assert record.data is not None
            assert isinstance(record.data, dict)


class TestFaultGeneratorIntegration:
    """Specific integration tests for Fault generator."""

    @pytest.mark.parametrize("persona_name", EXPECTED_PERSONAS)
    def test_fault_records_structure(self, persona_engine, persona_name):
        dataset = persona_engine.generate_single(
            persona_name=persona_name,
            dataset_type=DatasetType.FAULT,
            count=5,
            seed=42,
        )

        for record in dataset.records:
            assert "type" in record.data or "faults_injected" in record.data or "data" in record.data


class TestHighVolumeGeneration:
    """Tests for generating larger datasets."""

    @pytest.mark.parametrize("persona_name", EXPECTED_PERSONAS)
    def test_generate_100_records(self, persona_engine, persona_name):
        """Test generating 100 records doesn't fail."""
        dataset = persona_engine.generate_single(
            persona_name=persona_name,
            dataset_type=DatasetType.API,
            count=100,
            seed=42,
        )

        assert len(dataset.records) == 100

    @pytest.mark.parametrize("dataset_type", NON_FAULT_GENERATOR_TYPES)
    def test_generate_500_records(self, persona_engine, dataset_type):
        """Test generating 500 records with each non-fault generator type."""
        dataset = persona_engine.generate_single(
            persona_name="normal_user",
            dataset_type=dataset_type,
            count=500,
            seed=42,
        )

        assert len(dataset.records) == 500

    def test_generate_500_fault_records(self, persona_engine):
        """Test generating 500 fault records (includes baseline)."""
        dataset = persona_engine.generate_single(
            persona_name="normal_user",
            dataset_type=DatasetType.FAULT,
            count=500,
            seed=42,
        )

        # Fault generator adds 1 baseline + 500 faulty = 501 records
        assert len(dataset.records) == 501


class TestStreamingInterface:
    """Tests for the streaming generation interface."""

    @pytest.mark.parametrize("persona_name", EXPECTED_PERSONAS)
    @pytest.mark.parametrize("dataset_type", GENERATOR_TYPES)
    def test_stream_records(self, persona_engine, persona_name, dataset_type):
        """Test streaming interface for all combinations."""
        stream = persona_engine.stream(
            persona_name=persona_name,
            dataset_type=dataset_type,
            count=10,
            seed=42,
        )

        # stream() returns a generator, not None, when persona exists
        assert stream is not None

        records = list(stream)
        assert len(records) == 10

        for record in records:
            # stream() yields record.data (dict), not the record itself
            assert record is not None
            assert isinstance(record, dict)


class TestEdgeCases:
    """Edge case tests."""

    @pytest.mark.parametrize("dataset_type", NON_FAULT_GENERATOR_TYPES)
    def test_generate_single_record(self, persona_engine, dataset_type):
        """Test generating just 1 record with non-fault generators."""
        dataset = persona_engine.generate_single(
            persona_name="normal_user",
            dataset_type=dataset_type,
            count=1,
            seed=42,
        )

        assert len(dataset.records) == 1

    def test_generate_single_fault_record(self, persona_engine):
        """Test generating 1 fault record (plus baseline = 2 total)."""
        dataset = persona_engine.generate_single(
            persona_name="normal_user",
            dataset_type=DatasetType.FAULT,
            count=1,
            seed=42,
        )

        # Fault generator: 1 baseline + 1 faulty = 2 records
        assert len(dataset.records) == 2

    def test_generate_single_fault_record_no_baseline(self, persona_engine):
        """Test generating 1 fault record without baseline."""
        dataset = persona_engine.generate_single(
            persona_name="normal_user",
            dataset_type=DatasetType.FAULT,
            count=1,
            seed=42,
            include_valid_baseline=False,
        )

        assert len(dataset.records) == 1

    def test_nonexistent_persona_returns_none(self, persona_engine):
        """Test that requesting a non-existent persona returns None."""
        dataset = persona_engine.generate_single(
            persona_name="nonexistent_persona",
            dataset_type=DatasetType.API,
            count=10,
        )

        assert dataset is None

    @pytest.mark.parametrize("persona_name", EXPECTED_PERSONAS)
    def test_different_seeds_produce_different_output(self, persona_engine, persona_name):
        """Test that different seeds produce different data."""
        dataset1 = persona_engine.generate_single(
            persona_name=persona_name,
            dataset_type=DatasetType.API,
            count=5,
            seed=1,
        )

        dataset2 = persona_engine.generate_single(
            persona_name=persona_name,
            dataset_type=DatasetType.API,
            count=5,
            seed=2,
        )

        # At least some records should be different
        different = False
        for r1, r2 in zip(dataset1.records, dataset2.records):
            if r1.data != r2.data:
                different = True
                break

        assert different, f"Different seeds produced identical output for {persona_name}"
