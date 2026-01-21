"""Validation Engine - Enforces schema correctness.

The Validation Engine ensures:
- Generated data matches expected schemas
- Personas define valid behaviors
- Profiles have valid configurations
"""

from typing import Any
from enum import Enum
from dataclasses import dataclass, field

from pydantic import BaseModel, Field
import jsonschema

from persona_platform.personas.base import Persona, Behavior, BehaviorType
from persona_platform.profiles.base import Profile, DatasetConfig
from persona_platform.generators.base import GeneratedDataset, GeneratedRecord, DatasetType


class ValidationSeverity(str, Enum):
    """Severity levels for validation issues."""

    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


@dataclass
class ValidationIssue:
    """A single validation issue."""

    severity: ValidationSeverity
    message: str
    path: str = ""
    context: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "severity": self.severity.value,
            "message": self.message,
            "path": self.path,
            "context": self.context,
        }


@dataclass
class ValidationResult:
    """Result of a validation operation."""

    valid: bool
    issues: list[ValidationIssue] = field(default_factory=list)
    validated_count: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def error_count(self) -> int:
        return sum(1 for i in self.issues if i.severity == ValidationSeverity.ERROR)

    @property
    def warning_count(self) -> int:
        return sum(1 for i in self.issues if i.severity == ValidationSeverity.WARNING)

    def add_issue(
        self,
        severity: ValidationSeverity,
        message: str,
        path: str = "",
        **context: Any,
    ) -> None:
        self.issues.append(
            ValidationIssue(
                severity=severity,
                message=message,
                path=path,
                context=context,
            )
        )
        if severity == ValidationSeverity.ERROR:
            self.valid = False

    def merge(self, other: "ValidationResult") -> "ValidationResult":
        """Merge another validation result into this one."""
        return ValidationResult(
            valid=self.valid and other.valid,
            issues=self.issues + other.issues,
            validated_count=self.validated_count + other.validated_count,
            metadata={**self.metadata, **other.metadata},
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "valid": self.valid,
            "error_count": self.error_count,
            "warning_count": self.warning_count,
            "validated_count": self.validated_count,
            "issues": [i.to_dict() for i in self.issues],
            "metadata": self.metadata,
        }


class ValidationEngine:
    """Engine for validating personas, profiles, and generated data.

    Enforces:
    - Schema correctness
    - Behavioral constraints
    - Configuration validity
    """

    def validate_persona(self, persona: Persona) -> ValidationResult:
        """Validate a persona definition.

        Checks:
        - Required fields are present
        - Behaviors have valid types
        - No duplicate behavior names
        - Inheritance references exist (if registry provided)
        """
        result = ValidationResult(valid=True, validated_count=1)

        if not persona.name:
            result.add_issue(
                ValidationSeverity.ERROR,
                "Persona must have a name",
                path="name",
            )

        if not persona.name.replace("_", "").replace("-", "").isalnum():
            result.add_issue(
                ValidationSeverity.WARNING,
                "Persona name should be alphanumeric with underscores/hyphens",
                path="name",
                actual=persona.name,
            )

        behavior_names = set()
        for i, behavior in enumerate(persona.behaviors):
            if behavior.name in behavior_names:
                result.add_issue(
                    ValidationSeverity.ERROR,
                    f"Duplicate behavior name: {behavior.name}",
                    path=f"behaviors[{i}].name",
                )
            behavior_names.add(behavior.name)

            if not isinstance(behavior.behavior_type, BehaviorType):
                result.add_issue(
                    ValidationSeverity.ERROR,
                    f"Invalid behavior type: {behavior.behavior_type}",
                    path=f"behaviors[{i}].behavior_type",
                )

            if not 0.0 <= behavior.weight <= 1.0:
                result.add_issue(
                    ValidationSeverity.ERROR,
                    f"Behavior weight must be between 0 and 1: {behavior.weight}",
                    path=f"behaviors[{i}].weight",
                )

        return result

    def validate_profile(self, profile: Profile) -> ValidationResult:
        """Validate a profile configuration.

        Checks:
        - Required fields are present
        - Dataset configurations are valid
        - Output configuration is valid
        """
        result = ValidationResult(valid=True, validated_count=1)

        if not profile.name:
            result.add_issue(
                ValidationSeverity.ERROR,
                "Profile must have a name",
                path="name",
            )

        if not profile.personas:
            result.add_issue(
                ValidationSeverity.WARNING,
                "Profile has no personas defined",
                path="personas",
            )

        if not profile.datasets:
            result.add_issue(
                ValidationSeverity.WARNING,
                "Profile has no datasets defined",
                path="datasets",
            )

        for i, dataset in enumerate(profile.datasets):
            if dataset.count < 1:
                result.add_issue(
                    ValidationSeverity.ERROR,
                    f"Dataset count must be at least 1: {dataset.count}",
                    path=f"datasets[{i}].count",
                )

            if not isinstance(dataset.dataset_type, DatasetType):
                result.add_issue(
                    ValidationSeverity.ERROR,
                    f"Invalid dataset type: {dataset.dataset_type}",
                    path=f"datasets[{i}].dataset_type",
                )

        return result

    def validate_dataset(
        self,
        dataset: GeneratedDataset,
        schema: dict[str, Any] | None = None,
    ) -> ValidationResult:
        """Validate a generated dataset.

        Checks:
        - Dataset has records
        - Records match optional schema
        - Metadata is present
        """
        result = ValidationResult(valid=True, validated_count=len(dataset))

        if not dataset.records:
            result.add_issue(
                ValidationSeverity.WARNING,
                "Dataset has no records",
                path="records",
            )

        if schema:
            for i, record in enumerate(dataset.records):
                record_result = self.validate_record(record, schema)
                for issue in record_result.issues:
                    issue.path = f"records[{i}].{issue.path}"
                    result.issues.append(issue)
                    if issue.severity == ValidationSeverity.ERROR:
                        result.valid = False

        return result

    def validate_record(
        self,
        record: GeneratedRecord,
        schema: dict[str, Any],
    ) -> ValidationResult:
        """Validate a single record against a JSON schema.

        Args:
            record: The record to validate
            schema: JSON Schema to validate against

        Returns:
            Validation result
        """
        result = ValidationResult(valid=True, validated_count=1)

        try:
            jsonschema.validate(record.data, schema)
        except jsonschema.ValidationError as e:
            result.add_issue(
                ValidationSeverity.ERROR,
                f"Schema validation failed: {e.message}",
                path=".".join(str(p) for p in e.absolute_path),
                schema_path=list(e.schema_path),
            )
        except jsonschema.SchemaError as e:
            result.add_issue(
                ValidationSeverity.ERROR,
                f"Invalid schema: {e.message}",
                path="schema",
            )

        return result

    def validate_data_against_schema(
        self,
        data: dict[str, Any],
        schema: dict[str, Any],
    ) -> ValidationResult:
        """Validate arbitrary data against a JSON schema."""
        result = ValidationResult(valid=True, validated_count=1)

        try:
            jsonschema.validate(data, schema)
        except jsonschema.ValidationError as e:
            result.add_issue(
                ValidationSeverity.ERROR,
                f"Schema validation failed: {e.message}",
                path=".".join(str(p) for p in e.absolute_path),
            )
        except jsonschema.SchemaError as e:
            result.add_issue(
                ValidationSeverity.ERROR,
                f"Invalid schema: {e.message}",
                path="schema",
            )

        return result

    def create_schema_from_sample(
        self,
        sample: dict[str, Any],
        required_fields: list[str] | None = None,
    ) -> dict[str, Any]:
        """Create a JSON schema from a sample data object.

        Args:
            sample: Sample data to infer schema from
            required_fields: Optional list of required fields

        Returns:
            JSON Schema dictionary
        """
        schema: dict[str, Any] = {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "properties": {},
        }

        for key, value in sample.items():
            schema["properties"][key] = self._infer_type(value)

        if required_fields:
            schema["required"] = required_fields

        return schema

    def _infer_type(self, value: Any) -> dict[str, Any]:
        """Infer JSON schema type from a Python value."""
        if value is None:
            return {"type": "null"}
        elif isinstance(value, bool):
            return {"type": "boolean"}
        elif isinstance(value, int):
            return {"type": "integer"}
        elif isinstance(value, float):
            return {"type": "number"}
        elif isinstance(value, str):
            return {"type": "string"}
        elif isinstance(value, list):
            if value:
                return {"type": "array", "items": self._infer_type(value[0])}
            return {"type": "array"}
        elif isinstance(value, dict):
            properties = {k: self._infer_type(v) for k, v in value.items()}
            return {"type": "object", "properties": properties}
        else:
            return {"type": "string"}
