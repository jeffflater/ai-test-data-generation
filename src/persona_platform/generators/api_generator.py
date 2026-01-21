"""API Generator - Generates API request/response datasets.

Used for contract and integration testing.
"""

from typing import Any
from datetime import datetime, timezone
import uuid

from faker import Faker

from persona_platform.generators.base import (
    Generator,
    DatasetType,
    GeneratedDataset,
    GeneratedRecord,
)
from persona_platform.personas.base import Persona, Behavior, BehaviorType


class APIGenerator(Generator):
    """Generator for API CRUD datasets.

    Produces request/response pairs suitable for:
    - Contract testing
    - Integration testing
    - API mocking
    """

    dataset_type = DatasetType.API

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
        """Generate API dataset from a persona.

        Options:
            method: HTTP method (GET, POST, PUT, DELETE, PATCH)
            endpoint: API endpoint pattern
            include_response: Whether to include response data
            schema: Optional schema for field generation
        """
        method = options.get("method", "POST")
        endpoint = options.get("endpoint", "/api/resource")
        include_response = options.get("include_response", True)
        schema = options.get("schema", {})

        records = []
        for i in range(count):
            base_data = self._generate_base_data(
                method=method,
                endpoint=endpoint,
                include_response=include_response,
                schema=schema,
            )

            data, applied = self._apply_all_behaviors(base_data, persona)

            records.append(
                GeneratedRecord(
                    data=data,
                    metadata={
                        "behaviors_applied": applied,
                        "persona": persona.name,
                        "generator": "api",
                    },
                    sequence_number=i,
                )
            )

        return GeneratedDataset(
            dataset_type=DatasetType.API,
            records=records,
            persona_name=persona.name,
            seed=self._seed,
            total_count=count,
            metadata={"method": method, "endpoint": endpoint},
        )

    def _generate_base_data(self, **options: Any) -> dict[str, Any]:
        """Generate base API request/response data."""
        method = options.get("method", "POST")
        endpoint = options.get("endpoint", "/api/resource")
        include_response = options.get("include_response", True)
        schema = options.get("schema", {})

        request_id = self._generate_uuid()
        timestamp = datetime.now(timezone.utc).isoformat()

        request_body = self._generate_from_schema(schema) if schema else {
            "id": self._generate_uuid(),
            "name": self._faker.name(),
            "email": self._faker.email(),
            "created_at": timestamp,
        }

        data = {
            "request": {
                "method": method,
                "endpoint": endpoint,
                "headers": {
                    "Content-Type": "application/json",
                    "X-Request-ID": request_id,
                    "X-Timestamp": timestamp,
                },
                "body": request_body if method in ("POST", "PUT", "PATCH") else None,
                "query_params": {} if method != "GET" else {"limit": 10, "offset": 0},
            },
        }

        if include_response:
            data["response"] = {
                "status_code": 200,
                "headers": {
                    "Content-Type": "application/json",
                    "X-Request-ID": request_id,
                },
                "body": {
                    "success": True,
                    "data": request_body,
                    "timestamp": timestamp,
                },
            }

        return data

    def _generate_uuid(self) -> str:
        """Generate a deterministic UUID based on the seeded RNG."""
        return "%08x-%04x-%04x-%04x-%012x" % (
            self._rng.getrandbits(32),
            self._rng.getrandbits(16),
            self._rng.getrandbits(16),
            self._rng.getrandbits(16),
            self._rng.getrandbits(48),
        )

    def _generate_from_schema(self, schema: dict[str, Any]) -> dict[str, Any]:
        """Generate data from a schema definition."""
        result = {}
        for field, field_def in schema.items():
            result[field] = self._generate_field(field, field_def)
        return result

    def _generate_field(self, field_name: str, field_def: str | dict[str, Any]) -> Any:
        """Generate a single field value based on type or field definition."""
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
                    return [self._generate_field("item", items) for _ in range(count)]

                # Handle nested object fields
                if field_type == "object":
                    properties = field_def.get("properties", {})
                    if properties:
                        return self._generate_from_schema(properties)
                    return {}

                # Use format hint if available
                if field_format:
                    return self._generate_by_type(field_name, field_format)

                return self._generate_by_type(field_name, field_type)

            # Fallback: treat as nested object schema (old format)
            return self._generate_from_schema(field_def)

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
        if "username" in name_lower:
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
        type_map = {
            "string": lambda: self._faker.word(),
            "name": lambda: self._faker.name(),
            "email": lambda: self._faker.email(),
            "phone": lambda: self._faker.phone_number(),
            "address": lambda: self._faker.address(),
            "uuid": lambda: self._generate_uuid(),
            "int": lambda: self._rng.randint(1, 1000),
            "integer": lambda: self._rng.randint(1, 1000),
            "float": lambda: round(self._rng.uniform(0, 1000), 2),
            "bool": lambda: self._rng.choice([True, False]),
            "boolean": lambda: self._rng.choice([True, False]),
            "date": lambda: self._faker.date(),
            "datetime": lambda: datetime.now(timezone.utc).isoformat(),
            "date-time": lambda: datetime.now(timezone.utc).isoformat(),
            "timestamp": lambda: datetime.now(timezone.utc).isoformat(),
            "url": lambda: self._faker.url(),
            "text": lambda: self._faker.text(max_nb_chars=200),
            "city": lambda: self._faker.city(),
            "country": lambda: self._faker.country(),
        }

        # Try exact type match
        if field_type.lower() in type_map:
            return type_map[field_type.lower()]()

        return self._faker.word()

    def _transform_for_behavior(
        self,
        data: dict[str, Any],
        behavior: Behavior,
    ) -> dict[str, Any]:
        """Apply behavior-specific transformations to API data."""
        params = behavior.parameters

        if behavior.behavior_type == BehaviorType.LATENCY:
            delay_ms = params.get("delay_ms", self._rng.randint(100, 5000))
            data.setdefault("metadata", {})["simulated_latency_ms"] = delay_ms

        elif behavior.behavior_type == BehaviorType.ERROR_RATE:
            if "response" in data:
                error_codes = params.get("error_codes", [400, 500, 502, 503])
                error_code = self._rng.choice(error_codes)
                data["response"]["status_code"] = error_code
                data["response"]["body"] = {
                    "success": False,
                    "error": {
                        "code": error_code,
                        "message": self._get_error_message(error_code),
                    },
                }

        elif behavior.behavior_type == BehaviorType.DATA_QUALITY:
            quality_type = params.get("type", "missing_fields")
            if quality_type == "missing_fields":
                fields_to_remove = params.get("fields", [])
                if "request" in data and data["request"].get("body"):
                    for field in fields_to_remove:
                        data["request"]["body"].pop(field, None)
            elif quality_type == "invalid_format":
                field = params.get("field", "email")
                if "request" in data and data["request"].get("body"):
                    data["request"]["body"][field] = "invalid_format_" + self._faker.word()

        elif behavior.behavior_type == BehaviorType.TIMING:
            timing_pattern = params.get("pattern", "burst")
            data.setdefault("metadata", {})["timing_pattern"] = timing_pattern

        return data

    def _get_error_message(self, status_code: int) -> str:
        """Get an appropriate error message for a status code."""
        messages = {
            400: "Bad Request: Invalid input data",
            401: "Unauthorized: Authentication required",
            403: "Forbidden: Access denied",
            404: "Not Found: Resource does not exist",
            422: "Unprocessable Entity: Validation failed",
            429: "Too Many Requests: Rate limit exceeded",
            500: "Internal Server Error: Unexpected error occurred",
            502: "Bad Gateway: Upstream server error",
            503: "Service Unavailable: Server temporarily unavailable",
            504: "Gateway Timeout: Upstream server timeout",
        }
        return messages.get(status_code, f"Error: HTTP {status_code}")
