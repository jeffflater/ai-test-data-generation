"""Fault Generator - Generates faulty/mutated datasets for resilience testing.

Used for negative testing and fault injection.
"""

from typing import Any
from datetime import datetime, timezone
from enum import Enum
import uuid
import json
import copy

from faker import Faker

from persona_platform.generators.base import (
    Generator,
    DatasetType,
    GeneratedDataset,
    GeneratedRecord,
)
from persona_platform.personas.base import Persona, Behavior, BehaviorType


class FaultType(str, Enum):
    """Types of faults that can be injected."""

    NULL_VALUE = "null_value"
    EMPTY_VALUE = "empty_value"
    INVALID_TYPE = "invalid_type"
    OUT_OF_RANGE = "out_of_range"
    MALFORMED_JSON = "malformed_json"
    ENCODING_ERROR = "encoding_error"
    MISSING_FIELD = "missing_field"
    EXTRA_FIELD = "extra_field"
    DUPLICATE_KEY = "duplicate_key"
    OVERFLOW = "overflow"
    NEGATIVE_VALUE = "negative_value"
    SPECIAL_CHARACTERS = "special_characters"
    SQL_INJECTION = "sql_injection"
    XSS_PAYLOAD = "xss_payload"
    BOUNDARY_VALUE = "boundary_value"
    UNICODE_EDGE = "unicode_edge"


class FaultGenerator(Generator):
    """Generator for faulty/mutated datasets.

    Produces datasets with intentional faults for:
    - Negative testing
    - Resilience testing
    - Input validation testing
    - Error handling verification
    """

    dataset_type = DatasetType.FAULT

    def __init__(self, seed: int | None = None):
        super().__init__(seed)
        self._faker = Faker()
        if seed is not None:
            Faker.seed(seed)

    def generate(
        self,
        persona: Persona,
        count: int = 1,
        **options: Any,
    ) -> GeneratedDataset:
        """Generate faulty dataset from a persona.

        Options:
            base_schema: Schema for valid data before faults
            fault_types: List of fault types to inject
            fault_probability: Probability of fault per field (0.0-1.0)
            target_fields: Specific fields to target for faults
            include_valid_baseline: Include a valid record for comparison
        """
        base_schema = options.get("base_schema", self._default_schema())
        fault_types = options.get("fault_types", list(FaultType))
        fault_probability = options.get("fault_probability", 0.3)
        target_fields = options.get("target_fields", None)
        include_valid_baseline = options.get("include_valid_baseline", True)

        records = []

        if include_valid_baseline:
            valid_data = self._generate_valid_data(base_schema)
            records.append(
                GeneratedRecord(
                    data={
                        "type": "valid_baseline",
                        "data": valid_data,
                        "faults_injected": [],
                    },
                    metadata={
                        "behaviors_applied": [],
                        "persona": persona.name,
                        "generator": "fault",
                        "is_baseline": True,
                    },
                    sequence_number=0,
                )
            )

        start_seq = 1 if include_valid_baseline else 0

        for i in range(count):
            base_data = self._generate_valid_data(base_schema)

            faulty_data, faults = self._inject_faults(
                base_data,
                fault_types,
                fault_probability,
                target_fields,
            )

            data, applied = self._apply_all_behaviors(
                {"original": base_data, "faulty": faulty_data, "faults": faults},
                persona,
            )

            records.append(
                GeneratedRecord(
                    data={
                        "type": "faulty",
                        "original_data": data.get("original", base_data),
                        "faulty_data": data.get("faulty", faulty_data),
                        "faults_injected": data.get("faults", faults),
                    },
                    metadata={
                        "behaviors_applied": applied,
                        "persona": persona.name,
                        "generator": "fault",
                        "fault_count": len(faults),
                    },
                    sequence_number=i + start_seq,
                )
            )

        return GeneratedDataset(
            dataset_type=DatasetType.FAULT,
            records=records,
            persona_name=persona.name,
            seed=self._seed,
            total_count=len(records),
            metadata={
                "fault_types": [ft.value if isinstance(ft, FaultType) else ft for ft in fault_types],
                "fault_probability": fault_probability,
                "include_valid_baseline": include_valid_baseline,
            },
        )

    def generate_fault_matrix(
        self,
        base_schema: dict[str, str],
        fault_types: list[FaultType] | None = None,
    ) -> list[dict[str, Any]]:
        """Generate a matrix of all fault combinations.

        Args:
            base_schema: Schema for valid data
            fault_types: List of fault types to include

        Returns:
            List of fault scenarios with descriptions
        """
        if fault_types is None:
            fault_types = list(FaultType)

        matrix = []
        base_data = self._generate_valid_data(base_schema)

        for field in base_schema.keys():
            for fault_type in fault_types:
                faulty_data = copy.deepcopy(base_data)
                fault_info = self._apply_single_fault(faulty_data, field, fault_type)

                if fault_info:
                    matrix.append({
                        "field": field,
                        "fault_type": fault_type.value,
                        "original_value": base_data.get(field),
                        "faulty_value": faulty_data.get(field),
                        "description": fault_info.get("description", ""),
                        "expected_behavior": fault_info.get("expected_behavior", "validation_error"),
                    })

        return matrix

    def _default_schema(self) -> dict[str, str]:
        """Return default schema for fault generation."""
        return {
            "id": "uuid",
            "name": "string",
            "email": "email",
            "age": "integer",
            "balance": "float",
            "is_active": "boolean",
            "created_at": "datetime",
        }

    def _generate_valid_data(self, schema: dict[str, Any]) -> dict[str, Any]:
        """Generate valid data according to schema."""
        data = {}
        for field, field_def in schema.items():
            data[field] = self._generate_field_value(field, field_def)
        return data

    def _generate_field_value(self, field_name: str, field_def: str | dict[str, Any]) -> Any:
        """Generate a field value based on type or field definition."""
        # Handle string type name directly
        if isinstance(field_def, str):
            return self._generate_by_type(field_name, field_def)

        # Handle rich field definition with 'type' key
        if isinstance(field_def, dict):
            if "type" in field_def:
                field_type = field_def["type"]
                field_format = field_def.get("format")

                # Handle enum fields
                if "enum" in field_def and field_def["enum"]:
                    return self._rng.choice(field_def["enum"])

                # Handle array fields
                if field_type == "array":
                    items = field_def.get("items", {"type": "string"})
                    count = self._rng.randint(1, 3)
                    return [self._generate_field_value("item", items) for _ in range(count)]

                # Handle nested object fields
                if field_type == "object":
                    properties = field_def.get("properties", {})
                    if properties:
                        return {k: self._generate_field_value(k, v) for k, v in properties.items()}
                    return {}

                # Use format hint if available
                if field_format:
                    return self._generate_by_type(field_name, field_format)

                return self._generate_by_type(field_name, field_type)

            # Fallback: treat as nested object schema (old format)
            return {k: self._generate_field_value(k, v) for k, v in field_def.items()}

        # Fallback for unexpected types
        return self._faker.word()

    def _generate_by_type(self, field_name: str, field_type: str) -> Any:
        """Generate a value based on type name."""
        type_generators = {
            "uuid": lambda: str(uuid.uuid4()),
            "string": lambda: self._faker.word(),
            "name": lambda: self._faker.name(),
            "email": lambda: self._faker.email(),
            "integer": lambda: self._rng.randint(1, 100),
            "int": lambda: self._rng.randint(1, 100),
            "float": lambda: round(self._rng.uniform(0, 1000), 2),
            "number": lambda: round(self._rng.uniform(0, 1000), 2),
            "boolean": lambda: self._rng.choice([True, False]),
            "bool": lambda: self._rng.choice([True, False]),
            "datetime": lambda: datetime.now(timezone.utc).isoformat(),
            "date-time": lambda: datetime.now(timezone.utc).isoformat(),
            "timestamp-millis": lambda: datetime.now(timezone.utc).isoformat(),
            "date": lambda: self._faker.date(),
            "phone": lambda: self._faker.phone_number(),
            "url": lambda: self._faker.url(),
            "text": lambda: self._faker.text(max_nb_chars=200),
        }

        if field_type.lower() in type_generators:
            return type_generators[field_type.lower()]()
        return self._faker.word()

    def _inject_faults(
        self,
        data: dict[str, Any],
        fault_types: list[FaultType],
        fault_probability: float,
        target_fields: list[str] | None,
    ) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        """Inject faults into data."""
        faulty_data = copy.deepcopy(data)
        faults = []

        fields = target_fields or list(data.keys())

        for field in fields:
            if self._rng.random() < fault_probability:
                fault_type = self._rng.choice(fault_types)
                if isinstance(fault_type, str):
                    fault_type = FaultType(fault_type)

                fault_info = self._apply_single_fault(faulty_data, field, fault_type)
                if fault_info:
                    faults.append({
                        "field": field,
                        "fault_type": fault_type.value,
                        **fault_info,
                    })

        return faulty_data, faults

    def _apply_single_fault(
        self,
        data: dict[str, Any],
        field: str,
        fault_type: FaultType,
    ) -> dict[str, Any] | None:
        """Apply a single fault to a field."""
        if field not in data:
            return None

        original_value = data[field]
        fault_info: dict[str, Any] = {
            "original_value": original_value,
            "description": "",
            "expected_behavior": "validation_error",
        }

        if fault_type == FaultType.NULL_VALUE:
            data[field] = None
            fault_info["description"] = "Set field to null"

        elif fault_type == FaultType.EMPTY_VALUE:
            if isinstance(original_value, str):
                data[field] = ""
            elif isinstance(original_value, list):
                data[field] = []
            elif isinstance(original_value, dict):
                data[field] = {}
            else:
                data[field] = None
            fault_info["description"] = "Set field to empty value"

        elif fault_type == FaultType.INVALID_TYPE:
            if isinstance(original_value, str):
                data[field] = self._rng.randint(-999, 999)
            elif isinstance(original_value, (int, float)):
                data[field] = self._faker.word()
            elif isinstance(original_value, bool):
                data[field] = "not_a_boolean"
            else:
                data[field] = {"invalid": "type"}
            fault_info["description"] = "Changed field to invalid type"

        elif fault_type == FaultType.OUT_OF_RANGE:
            if isinstance(original_value, int):
                data[field] = self._rng.choice([-(2**31), 2**31 - 1, -(2**63), 2**63 - 1])
            elif isinstance(original_value, float):
                data[field] = self._rng.choice([float("inf"), float("-inf"), float("nan")])
            else:
                data[field] = "x" * 100000
            fault_info["description"] = "Set field to out-of-range value"

        elif fault_type == FaultType.MALFORMED_JSON:
            data[field] = '{"broken": json'
            fault_info["description"] = "Set field to malformed JSON string"

        elif fault_type == FaultType.ENCODING_ERROR:
            data[field] = f"{original_value}\x00\xff\xfe"
            fault_info["description"] = "Added invalid encoding characters"

        elif fault_type == FaultType.MISSING_FIELD:
            del data[field]
            fault_info["description"] = "Removed field entirely"
            fault_info["faulty_value"] = "<MISSING>"

        elif fault_type == FaultType.EXTRA_FIELD:
            data[f"{field}_extra_unexpected"] = "unexpected_value"
            fault_info["description"] = "Added unexpected extra field"
            fault_info["expected_behavior"] = "should_ignore_or_reject"

        elif fault_type == FaultType.OVERFLOW:
            if isinstance(original_value, (int, float)):
                data[field] = 10**308
            else:
                data[field] = "A" * 10000000
            fault_info["description"] = "Set to overflow value"

        elif fault_type == FaultType.NEGATIVE_VALUE:
            if isinstance(original_value, (int, float)):
                data[field] = -abs(original_value) - 1
            else:
                data[field] = -1
            fault_info["description"] = "Set to negative value"

        elif fault_type == FaultType.SPECIAL_CHARACTERS:
            special = '<script>alert("xss")</script>; DROP TABLE users; --\x00\n\r\t'
            data[field] = special
            fault_info["description"] = "Injected special characters"

        elif fault_type == FaultType.SQL_INJECTION:
            data[field] = "'; DROP TABLE users; --"
            fault_info["description"] = "SQL injection payload"
            fault_info["expected_behavior"] = "should_sanitize"

        elif fault_type == FaultType.XSS_PAYLOAD:
            data[field] = '<script>alert("XSS")</script>'
            fault_info["description"] = "XSS payload"
            fault_info["expected_behavior"] = "should_escape"

        elif fault_type == FaultType.BOUNDARY_VALUE:
            if isinstance(original_value, int):
                data[field] = self._rng.choice([0, -1, 1, 2**31 - 1, -(2**31)])
            elif isinstance(original_value, str):
                data[field] = self._rng.choice(["", " ", "a", "a" * 255])
            fault_info["description"] = "Set to boundary value"

        elif fault_type == FaultType.UNICODE_EDGE:
            unicode_samples = [
                "\u0000",
                "\uffff",
                "\U0001F4A9",
                "\u202e",
                "\u200b",
                "Ṫ̈́ḛ̋s̈́ẗ̈",
                "\u0300\u0301\u0302",
            ]
            data[field] = self._rng.choice(unicode_samples) + str(original_value)
            fault_info["description"] = "Added unicode edge case characters"

        fault_info["faulty_value"] = data.get(field, "<MISSING>")
        return fault_info

    def _transform_for_behavior(
        self,
        data: dict[str, Any],
        behavior: Behavior,
    ) -> dict[str, Any]:
        """Apply behavior-specific transformations to fault data."""
        params = behavior.parameters

        if behavior.behavior_type == BehaviorType.MUTATION:
            mutation_rate = params.get("rate", 0.5)
            if self._rng.random() < mutation_rate:
                if "faulty" in data:
                    fields = list(data["faulty"].keys())
                    if fields:
                        field = self._rng.choice(fields)
                        fault_type = self._rng.choice(list(FaultType))
                        self._apply_single_fault(data["faulty"], field, fault_type)
                        data.setdefault("faults", []).append({
                            "field": field,
                            "fault_type": fault_type.value,
                            "source": "behavior_mutation",
                        })

        elif behavior.behavior_type == BehaviorType.ERROR_RATE:
            severity = params.get("severity", "medium")
            severity_multipliers = {"low": 1, "medium": 2, "high": 4}
            multiplier = severity_multipliers.get(severity, 2)

            if "faulty" in data:
                for _ in range(multiplier - 1):
                    fields = list(data["faulty"].keys())
                    if fields:
                        field = self._rng.choice(fields)
                        fault_type = self._rng.choice(list(FaultType))
                        self._apply_single_fault(data["faulty"], field, fault_type)

        elif behavior.behavior_type == BehaviorType.PATTERN:
            pattern = params.get("pattern", "random")
            if pattern == "systematic" and "faults" in data:
                for fault in data["faults"]:
                    fault["systematic"] = True

        return data
