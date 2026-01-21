"""File Generator - Generates file-based datasets.

Used for ingest and ETL validation.
"""

from typing import Any
from datetime import datetime, timezone
from pathlib import Path
import uuid
import json
import csv
import io

from faker import Faker

from persona_platform.generators.base import (
    Generator,
    DatasetType,
    GeneratedDataset,
    GeneratedRecord,
)
from persona_platform.personas.base import Persona, Behavior, BehaviorType


class FileGenerator(Generator):
    """Generator for file-based datasets.

    Produces files suitable for:
    - Ingest pipeline testing
    - ETL validation
    - Batch processing testing
    """

    dataset_type = DatasetType.FILE

    SUPPORTED_FORMATS = ["json", "jsonl", "csv", "parquet_schema", "xml"]

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
        """Generate file dataset from a persona.

        Options:
            format: File format (json, jsonl, csv, parquet_schema, xml)
            schema: Data schema definition
            filename_pattern: Pattern for generated filenames
            include_headers: For CSV, whether to include headers
        """
        file_format = options.get("format", "json")
        schema = options.get("schema", self._default_schema())
        filename_pattern = options.get("filename_pattern", "data_{timestamp}_{sequence}")
        include_headers = options.get("include_headers", True)

        records = []
        batch_id = str(uuid.uuid4())[:8]
        timestamp = datetime.now(timezone.utc)

        for i in range(count):
            base_data = self._generate_base_data(
                schema=schema,
                sequence=i,
            )

            data, applied = self._apply_all_behaviors(base_data, persona)

            filename = filename_pattern.format(
                timestamp=timestamp.strftime("%Y%m%d_%H%M%S"),
                sequence=i,
                batch_id=batch_id,
                format=file_format,
            )
            if not filename.endswith(f".{file_format}"):
                filename = f"{filename}.{file_format}"

            file_record = {
                "filename": filename,
                "format": file_format,
                "content": data,
                "size_bytes": len(json.dumps(data)),
                "checksum": self._generate_checksum(data),
                "created_at": timestamp.isoformat(),
            }

            records.append(
                GeneratedRecord(
                    data=file_record,
                    metadata={
                        "behaviors_applied": applied,
                        "persona": persona.name,
                        "generator": "file",
                        "format": file_format,
                    },
                    sequence_number=i,
                )
            )

        return GeneratedDataset(
            dataset_type=DatasetType.FILE,
            records=records,
            persona_name=persona.name,
            seed=self._seed,
            total_count=count,
            metadata={
                "format": file_format,
                "batch_id": batch_id,
                "include_headers": include_headers,
            },
        )

    def generate_file_content(
        self,
        persona: Persona,
        row_count: int = 100,
        **options: Any,
    ) -> str:
        """Generate actual file content as a string.

        Args:
            persona: The persona defining behaviors
            row_count: Number of rows in the file
            **options: Generator-specific options

        Returns:
            File content as string
        """
        file_format = options.get("format", "json")
        schema = options.get("schema", self._default_schema())

        rows = []
        for i in range(row_count):
            base_data = self._generate_base_data(schema=schema, sequence=i)
            data, _ = self._apply_all_behaviors(base_data, persona)
            rows.append(data)

        if file_format == "json":
            return json.dumps(rows, indent=2)
        elif file_format == "jsonl":
            return "\n".join(json.dumps(row) for row in rows)
        elif file_format == "csv":
            return self._to_csv(rows, options.get("include_headers", True))
        elif file_format == "xml":
            return self._to_xml(rows)
        else:
            return json.dumps(rows)

    def _default_schema(self) -> dict[str, str]:
        """Return default schema for file generation."""
        return {
            "id": "uuid",
            "name": "name",
            "email": "email",
            "amount": "float",
            "status": "string",
            "created_at": "datetime",
        }

    def _generate_base_data(self, **options: Any) -> dict[str, Any]:
        """Generate base file record data."""
        schema = options.get("schema", self._default_schema())
        sequence = options.get("sequence", 0)

        data = {"_row_number": sequence}

        for field, field_type in schema.items():
            data[field] = self._generate_field_value(field, field_type)

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
                    count = self._rng.randint(1, 5)
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
        # First, try to infer from field name (higher priority for semantic names)
        name_lower = field_name.lower()
        if "email" in name_lower:
            return self._faker.email()
        if "full_name" in name_lower or field_name == "name":
            return self._faker.name()
        if "username" in name_lower or "user_name" in name_lower:
            return self._faker.user_name()
        if "phone" in name_lower:
            return self._faker.phone_number()
        if "city" in name_lower:
            return self._faker.city()
        if "country" in name_lower:
            return self._faker.country()
        if "street" in name_lower:
            return self._faker.street_address()
        if "zip" in name_lower or "postal" in name_lower:
            return self._faker.postcode()
        if "state" in name_lower:
            return self._faker.state()

        # Type-based generation
        type_generators = {
            "uuid": lambda: str(uuid.uuid4()),
            "string": lambda: self._faker.word(),
            "name": lambda: self._faker.name(),
            "email": lambda: self._faker.email(),
            "phone": lambda: self._faker.phone_number(),
            "address": lambda: self._faker.address().replace("\n", ", "),
            "int": lambda: self._rng.randint(1, 10000),
            "integer": lambda: self._rng.randint(1, 10000),
            "float": lambda: round(self._rng.uniform(0, 10000), 2),
            "number": lambda: round(self._rng.uniform(0, 10000), 2),
            "decimal": lambda: round(self._rng.uniform(0, 10000), 2),
            "bool": lambda: self._rng.choice([True, False]),
            "boolean": lambda: self._rng.choice([True, False]),
            "date": lambda: self._faker.date(),
            "datetime": lambda: datetime.now(timezone.utc).isoformat(),
            "date-time": lambda: datetime.now(timezone.utc).isoformat(),
            "timestamp": lambda: datetime.now(timezone.utc).isoformat(),
            "timestamp-millis": lambda: datetime.now(timezone.utc).isoformat(),
            "text": lambda: self._faker.text(max_nb_chars=500),
            "company": lambda: self._faker.company(),
            "country": lambda: self._faker.country(),
            "city": lambda: self._faker.city(),
            "status": lambda: self._rng.choice(["active", "inactive", "pending", "completed"]),
        }

        if field_type.lower() in type_generators:
            return type_generators[field_type.lower()]()

        return self._faker.word()

    def _generate_checksum(self, data: dict[str, Any]) -> str:
        """Generate a simple checksum for data."""
        import hashlib
        content = json.dumps(data, sort_keys=True)
        return hashlib.md5(content.encode()).hexdigest()

    def _to_csv(self, rows: list[dict[str, Any]], include_headers: bool = True) -> str:
        """Convert rows to CSV format."""
        if not rows:
            return ""

        output = io.StringIO()
        fieldnames = [k for k in rows[0].keys() if not k.startswith("_")]
        writer = csv.DictWriter(output, fieldnames=fieldnames, extrasaction="ignore")

        if include_headers:
            writer.writeheader()

        for row in rows:
            clean_row = {k: v for k, v in row.items() if not k.startswith("_")}
            writer.writerow(clean_row)

        return output.getvalue()

    def _to_xml(self, rows: list[dict[str, Any]]) -> str:
        """Convert rows to XML format."""
        lines = ['<?xml version="1.0" encoding="UTF-8"?>', "<records>"]

        for row in rows:
            lines.append("  <record>")
            for key, value in row.items():
                if not key.startswith("_"):
                    escaped_value = str(value).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
                    lines.append(f"    <{key}>{escaped_value}</{key}>")
            lines.append("  </record>")

        lines.append("</records>")
        return "\n".join(lines)

    def _transform_for_behavior(
        self,
        data: dict[str, Any],
        behavior: Behavior,
    ) -> dict[str, Any]:
        """Apply behavior-specific transformations to file data."""
        params = behavior.parameters

        if behavior.behavior_type == BehaviorType.DATA_QUALITY:
            issue_type = params.get("type", "missing_values")

            if issue_type == "missing_values":
                fields = params.get("fields", [])
                if not fields:
                    available_fields = [k for k in data.keys() if not k.startswith("_")]
                    if available_fields:
                        fields = [self._rng.choice(available_fields)]
                for field in fields:
                    if field in data:
                        data[field] = None

            elif issue_type == "invalid_format":
                field = params.get("field")
                if field and field in data:
                    data[field] = f"INVALID_{self._faker.word()}"

            elif issue_type == "encoding_error":
                field = params.get("field")
                if field and field in data and isinstance(data[field], str):
                    data[field] = data[field] + "\x00\xff"

            elif issue_type == "truncated":
                field = params.get("field")
                max_len = params.get("max_length", 10)
                if field and field in data and isinstance(data[field], str):
                    data[field] = data[field][:max_len]

        elif behavior.behavior_type == BehaviorType.PATTERN:
            pattern_type = params.get("type", "sequential")

            if pattern_type == "duplicate":
                data["_is_duplicate"] = True

            elif pattern_type == "out_of_sequence":
                if "_row_number" in data:
                    data["_row_number"] = self._rng.randint(0, 10000)

        elif behavior.behavior_type == BehaviorType.CONSTRAINT:
            constraint_type = params.get("type", "violate_unique")

            if constraint_type == "violate_unique":
                field = params.get("field", "id")
                if field in data:
                    data[field] = "DUPLICATE_" + str(data[field])[:20]

            elif constraint_type == "violate_foreign_key":
                field = params.get("field")
                if field and field in data:
                    data[field] = "INVALID_FK_" + str(uuid.uuid4())[:8]

        elif behavior.behavior_type == BehaviorType.VOLUME:
            data["_high_volume_marker"] = True

        return data
