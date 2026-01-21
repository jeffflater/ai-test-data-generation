"""Tests for the Generators module."""

import pytest
from persona_platform.personas.base import Persona, Behavior, BehaviorType
from persona_platform.generators.base import DatasetType, GeneratedDataset
from persona_platform.generators.api_generator import APIGenerator
from persona_platform.generators.streaming_generator import StreamingGenerator
from persona_platform.generators.file_generator import FileGenerator
from persona_platform.generators.load_generator import LoadGenerator, LoadProfile
from persona_platform.generators.fault_generator import FaultGenerator, FaultType
from persona_platform.generators.registry import GeneratorRegistry


@pytest.fixture
def simple_persona():
    """Create a simple persona for testing."""
    return Persona(
        name="test_persona",
        behaviors=[
            Behavior(
                name="test_timing",
                behavior_type=BehaviorType.TIMING,
                parameters={"pattern": "normal"},
                weight=0.5,
            ),
        ],
    )


@pytest.fixture
def error_persona():
    """Create a persona with error behavior."""
    return Persona(
        name="error_persona",
        behaviors=[
            Behavior(
                name="high_error_rate",
                behavior_type=BehaviorType.ERROR_RATE,
                parameters={"error_codes": [500, 502]},
                weight=1.0,
            ),
        ],
    )


class TestAPIGenerator:
    """Tests for APIGenerator."""

    def test_generate_basic(self, simple_persona):
        generator = APIGenerator(seed=42)
        dataset = generator.generate(simple_persona, count=10)

        assert isinstance(dataset, GeneratedDataset)
        assert dataset.dataset_type == DatasetType.API
        assert len(dataset.records) == 10
        assert dataset.persona_name == "test_persona"

    def test_generate_with_options(self, simple_persona):
        generator = APIGenerator(seed=42)
        dataset = generator.generate(
            simple_persona,
            count=5,
            method="PUT",
            endpoint="/api/users/{id}",
        )

        assert len(dataset.records) == 5
        for record in dataset.records:
            assert record.data["request"]["method"] == "PUT"
            assert record.data["request"]["endpoint"] == "/api/users/{id}"

    def test_generate_with_schema(self, simple_persona):
        generator = APIGenerator(seed=42)
        schema = {"name": "name", "email": "email", "age": "int"}
        dataset = generator.generate(simple_persona, count=3, schema=schema)

        for record in dataset.records:
            body = record.data["request"]["body"]
            assert "name" in body
            assert "email" in body
            assert "age" in body

    def test_deterministic_with_seed(self, simple_persona):
        gen1 = APIGenerator(seed=123)
        gen2 = APIGenerator(seed=123)

        dataset1 = gen1.generate(simple_persona, count=5)
        dataset2 = gen2.generate(simple_persona, count=5)

        for r1, r2 in zip(dataset1.records, dataset2.records):
            assert r1.data["request"]["headers"]["X-Request-ID"] == \
                   r2.data["request"]["headers"]["X-Request-ID"]

    def test_error_behavior_applied(self, error_persona):
        generator = APIGenerator(seed=42)
        dataset = generator.generate(error_persona, count=10)

        error_count = sum(
            1 for r in dataset.records
            if r.data.get("response", {}).get("status_code", 200) >= 400
        )
        assert error_count > 0


class TestStreamingGenerator:
    """Tests for StreamingGenerator."""

    def test_generate_basic(self, simple_persona):
        generator = StreamingGenerator(seed=42)
        dataset = generator.generate(simple_persona, count=100)

        assert dataset.dataset_type == DatasetType.STREAMING
        assert len(dataset.records) == 100

    def test_generate_with_topic(self, simple_persona):
        generator = StreamingGenerator(seed=42)
        dataset = generator.generate(
            simple_persona,
            count=10,
            topic="test-events",
            partition_count=6,
        )

        for record in dataset.records:
            assert record.data["topic"] == "test-events"
            assert 0 <= record.data["partition"] < 6

    def test_event_types(self, simple_persona):
        generator = StreamingGenerator(seed=42)

        for event_type in ["user_action", "sensor_reading", "transaction", "log"]:
            dataset = generator.generate(
                simple_persona,
                count=5,
                event_type=event_type,
            )
            assert len(dataset.records) == 5


class TestFileGenerator:
    """Tests for FileGenerator."""

    def test_generate_basic(self, simple_persona):
        generator = FileGenerator(seed=42)
        dataset = generator.generate(simple_persona, count=10)

        assert dataset.dataset_type == DatasetType.FILE
        assert len(dataset.records) == 10

    def test_generate_with_schema(self, simple_persona):
        generator = FileGenerator(seed=42)
        schema = {
            "id": "uuid",
            "name": "name",
            "amount": "float",
            "active": "boolean",
        }
        dataset = generator.generate(simple_persona, count=5, schema=schema)

        for record in dataset.records:
            content = record.data["content"]
            assert "id" in content
            assert "name" in content
            assert "amount" in content
            assert "active" in content

    def test_generate_file_content_json(self, simple_persona):
        generator = FileGenerator(seed=42)
        content = generator.generate_file_content(
            simple_persona,
            row_count=5,
            format="json",
        )

        import json
        data = json.loads(content)
        assert len(data) == 5

    def test_generate_file_content_csv(self, simple_persona):
        generator = FileGenerator(seed=42)
        content = generator.generate_file_content(
            simple_persona,
            row_count=5,
            format="csv",
        )

        lines = content.strip().split("\n")
        assert len(lines) == 6  # Header + 5 rows


class TestLoadGenerator:
    """Tests for LoadGenerator."""

    def test_generate_basic(self, simple_persona):
        generator = LoadGenerator(seed=42)
        dataset = generator.generate(simple_persona, count=100)

        assert dataset.dataset_type == DatasetType.LOAD
        assert len(dataset.records) == 100

    def test_generate_with_profile(self, simple_persona):
        generator = LoadGenerator(seed=42)
        dataset = generator.generate(
            simple_persona,
            count=50,
            load_profile="ramp",
            target_endpoint="/api/test",
        )

        assert dataset.metadata["load_profile"] == "ramp"
        for record in dataset.records:
            assert record.data["target"] == "/api/test"

    def test_load_profiles(self):
        profiles = ["constant", "ramp", "spike", "wave", "step", "soak"]
        for profile_name in profiles:
            assert profile_name in LoadGenerator.LOAD_PROFILES

    def test_load_profile_rate_calculation(self):
        profile = LoadProfile(
            name="test",
            base_rate=10,
            peak_rate=100,
            ramp_up_seconds=10,
            duration_seconds=60,
            pattern="ramp",
        )

        assert profile.get_rate_at(0) == 10
        assert profile.get_rate_at(5) == 55
        assert profile.get_rate_at(10) == 100

    def test_generate_load_plan(self, simple_persona):
        generator = LoadGenerator(seed=42)
        plan = generator.generate_load_plan(simple_persona, "ramp")

        assert len(plan) > 0
        assert "elapsed_seconds" in plan[0]
        assert "target_rate" in plan[0]


class TestFaultGenerator:
    """Tests for FaultGenerator."""

    def test_generate_basic(self, simple_persona):
        generator = FaultGenerator(seed=42)
        dataset = generator.generate(simple_persona, count=10)

        assert dataset.dataset_type == DatasetType.FAULT

    def test_includes_valid_baseline(self, simple_persona):
        generator = FaultGenerator(seed=42)
        dataset = generator.generate(
            simple_persona,
            count=5,
            include_valid_baseline=True,
        )

        baseline = dataset.records[0]
        assert baseline.data["type"] == "valid_baseline"

    def test_fault_types_applied(self, simple_persona):
        generator = FaultGenerator(seed=42)
        dataset = generator.generate(
            simple_persona,
            count=20,
            fault_probability=1.0,
        )

        for record in dataset.records:
            if record.data["type"] == "faulty":
                assert len(record.data["faults_injected"]) > 0

    def test_generate_fault_matrix(self):
        generator = FaultGenerator(seed=42)
        schema = {"name": "string", "age": "integer", "email": "email"}

        matrix = generator.generate_fault_matrix(schema)

        assert len(matrix) > 0
        for entry in matrix:
            assert "field" in entry
            assert "fault_type" in entry


class TestGeneratorRegistry:
    """Tests for GeneratorRegistry."""

    def test_default_generators_registered(self):
        registry = GeneratorRegistry()

        assert DatasetType.API in registry
        assert DatasetType.STREAMING in registry
        assert DatasetType.FILE in registry
        assert DatasetType.LOAD in registry
        assert DatasetType.FAULT in registry

    def test_create_generator(self):
        registry = GeneratorRegistry()
        generator = registry.create(DatasetType.API, seed=42)

        assert generator is not None
        assert isinstance(generator, APIGenerator)

    def test_list_types(self):
        registry = GeneratorRegistry()
        types = registry.list_types()

        assert len(types) == 5
        assert DatasetType.API in types
