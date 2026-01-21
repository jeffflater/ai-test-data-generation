"""Streaming Generator - Generates streaming event datasets.

Used for Kafka, Flink, and similar streaming simulations.
"""

from typing import Any, Iterator
from datetime import datetime, timezone, timedelta
import uuid
import json

from faker import Faker

from persona_platform.generators.base import (
    Generator,
    DatasetType,
    GeneratedDataset,
    GeneratedRecord,
)
from persona_platform.personas.base import Persona, Behavior, BehaviorType


class StreamingGenerator(Generator):
    """Generator for streaming event datasets.

    Produces event streams suitable for:
    - Kafka topic simulation
    - Flink job testing
    - Event-driven architecture testing
    """

    dataset_type = DatasetType.STREAMING

    def __init__(self, seed: int | None = None):
        super().__init__(seed)
        self._faker = Faker()
        if seed is not None:
            Faker.seed(seed)
        self._event_time = datetime.now(timezone.utc)

    def generate(
        self,
        persona: Persona,
        count: int = 1,
        **options: Any,
    ) -> GeneratedDataset:
        """Generate streaming dataset from a persona.

        Options:
            topic: Kafka topic name
            partition_count: Number of partitions
            event_type: Type of events to generate
            time_window_ms: Time window for events in milliseconds
            include_key: Whether to include partition key
        """
        topic = options.get("topic", "events")
        partition_count = options.get("partition_count", 3)
        event_type = options.get("event_type", "generic")
        time_window_ms = options.get("time_window_ms", 60000)
        include_key = options.get("include_key", True)

        records = []
        base_time = self._event_time

        for i in range(count):
            offset_ms = self._rng.randint(0, time_window_ms)
            event_time = base_time + timedelta(milliseconds=offset_ms)

            base_data = self._generate_base_data(
                topic=topic,
                partition_count=partition_count,
                event_type=event_type,
                include_key=include_key,
                event_time=event_time,
                sequence=i,
            )

            data, applied = self._apply_all_behaviors(base_data, persona)

            records.append(
                GeneratedRecord(
                    data=data,
                    metadata={
                        "behaviors_applied": applied,
                        "persona": persona.name,
                        "generator": "streaming",
                    },
                    sequence_number=i,
                )
            )

        records.sort(key=lambda r: r.data.get("timestamp", ""))

        return GeneratedDataset(
            dataset_type=DatasetType.STREAMING,
            records=records,
            persona_name=persona.name,
            seed=self._seed,
            total_count=count,
            metadata={"topic": topic, "partition_count": partition_count},
        )

    def stream_continuous(
        self,
        persona: Persona,
        events_per_second: float = 10.0,
        **options: Any,
    ) -> Iterator[GeneratedRecord]:
        """Generate a continuous stream of events.

        Args:
            persona: The persona defining behaviors
            events_per_second: Target event rate
            **options: Generator-specific options

        Yields:
            Events with realistic timing metadata
        """
        interval_ms = 1000.0 / events_per_second
        sequence = 0

        while True:
            jitter = self._rng.gauss(0, interval_ms * 0.1)
            actual_interval = max(1, interval_ms + jitter)

            self._event_time += timedelta(milliseconds=actual_interval)

            record = self.generate_single(
                persona,
                sequence_number=sequence,
                event_time=self._event_time,
                **options,
            )
            yield record
            sequence += 1

    def _generate_base_data(self, **options: Any) -> dict[str, Any]:
        """Generate base streaming event data."""
        topic = options.get("topic", "events")
        partition_count = options.get("partition_count", 3)
        event_type = options.get("event_type", "generic")
        include_key = options.get("include_key", True)
        event_time = options.get("event_time", datetime.now(timezone.utc))
        sequence = options.get("sequence", 0)

        event_id = str(uuid.uuid4())
        partition_key = self._faker.uuid4() if include_key else None
        partition = hash(partition_key) % partition_count if partition_key else 0

        payload = self._generate_event_payload(event_type)

        data = {
            "topic": topic,
            "partition": partition,
            "offset": sequence,
            "timestamp": event_time.isoformat(),
            "timestamp_type": "CREATE_TIME",
            "headers": {
                "event_id": event_id,
                "event_type": event_type,
                "correlation_id": str(uuid.uuid4()),
                "source": f"persona-platform-{persona.name if 'persona' in dir() else 'unknown'}",
            },
            "key": partition_key,
            "value": payload,
        }

        return data

    def _generate_event_payload(self, event_type: str) -> dict[str, Any]:
        """Generate event-type specific payload."""
        event_generators = {
            "user_action": self._generate_user_action_event,
            "sensor_reading": self._generate_sensor_event,
            "transaction": self._generate_transaction_event,
            "log": self._generate_log_event,
            "metric": self._generate_metric_event,
            "generic": self._generate_generic_event,
        }

        generator = event_generators.get(event_type, self._generate_generic_event)
        return generator()

    def _generate_user_action_event(self) -> dict[str, Any]:
        """Generate a user action event."""
        actions = ["click", "view", "purchase", "add_to_cart", "search", "login", "logout"]
        return {
            "user_id": str(uuid.uuid4()),
            "session_id": str(uuid.uuid4()),
            "action": self._rng.choice(actions),
            "page": self._faker.uri_path(),
            "metadata": {
                "user_agent": self._faker.user_agent(),
                "ip_address": self._faker.ipv4(),
                "country": self._faker.country_code(),
            },
        }

    def _generate_sensor_event(self) -> dict[str, Any]:
        """Generate a sensor reading event."""
        return {
            "device_id": f"sensor-{self._rng.randint(1000, 9999)}",
            "sensor_type": self._rng.choice(["temperature", "humidity", "pressure", "motion"]),
            "value": round(self._rng.uniform(0, 100), 2),
            "unit": self._rng.choice(["celsius", "percent", "hpa", "boolean"]),
            "battery_level": self._rng.randint(0, 100),
            "location": {
                "lat": float(self._faker.latitude()),
                "lon": float(self._faker.longitude()),
            },
        }

    def _generate_transaction_event(self) -> dict[str, Any]:
        """Generate a transaction event."""
        return {
            "transaction_id": str(uuid.uuid4()),
            "account_id": str(uuid.uuid4()),
            "amount": round(self._rng.uniform(1, 10000), 2),
            "currency": self._rng.choice(["USD", "EUR", "GBP", "JPY"]),
            "type": self._rng.choice(["credit", "debit", "transfer"]),
            "merchant": self._faker.company(),
            "status": "pending",
        }

    def _generate_log_event(self) -> dict[str, Any]:
        """Generate a log event."""
        levels = ["DEBUG", "INFO", "WARN", "ERROR"]
        return {
            "level": self._rng.choice(levels),
            "logger": f"com.example.{self._faker.word()}.{self._faker.word()}",
            "message": self._faker.sentence(),
            "thread": f"thread-{self._rng.randint(1, 100)}",
            "context": {
                "request_id": str(uuid.uuid4()),
                "user_id": str(uuid.uuid4()),
            },
        }

    def _generate_metric_event(self) -> dict[str, Any]:
        """Generate a metric event."""
        metric_names = ["cpu_usage", "memory_usage", "request_count", "error_rate", "latency_p99"]
        return {
            "metric_name": self._rng.choice(metric_names),
            "value": round(self._rng.uniform(0, 100), 2),
            "tags": {
                "host": self._faker.hostname(),
                "service": self._faker.word(),
                "environment": self._rng.choice(["prod", "staging", "dev"]),
            },
        }

    def _generate_generic_event(self) -> dict[str, Any]:
        """Generate a generic event."""
        return {
            "id": str(uuid.uuid4()),
            "type": self._faker.word(),
            "data": {
                "field1": self._faker.word(),
                "field2": self._rng.randint(1, 100),
                "field3": self._rng.choice([True, False]),
            },
        }

    def _transform_for_behavior(
        self,
        data: dict[str, Any],
        behavior: Behavior,
    ) -> dict[str, Any]:
        """Apply behavior-specific transformations to streaming data."""
        params = behavior.parameters

        if behavior.behavior_type == BehaviorType.LATENCY:
            delay_ms = params.get("delay_ms", self._rng.randint(100, 5000))
            if "timestamp" in data:
                original_time = datetime.fromisoformat(data["timestamp"].replace("Z", "+00:00"))
                delayed_time = original_time + timedelta(milliseconds=delay_ms)
                data["processing_timestamp"] = delayed_time.isoformat()
                data["headers"]["latency_ms"] = str(delay_ms)

        elif behavior.behavior_type == BehaviorType.TIMING:
            pattern = params.get("pattern", "normal")
            if pattern == "out_of_order":
                offset_ms = self._rng.randint(-5000, 5000)
                if "timestamp" in data:
                    original_time = datetime.fromisoformat(data["timestamp"].replace("Z", "+00:00"))
                    adjusted_time = original_time + timedelta(milliseconds=offset_ms)
                    data["timestamp"] = adjusted_time.isoformat()
                    data["headers"]["out_of_order"] = "true"
            elif pattern == "duplicate":
                data["headers"]["duplicate"] = "true"

        elif behavior.behavior_type == BehaviorType.DATA_QUALITY:
            quality_issue = params.get("issue", "null_fields")
            if quality_issue == "null_fields":
                if "value" in data and isinstance(data["value"], dict):
                    keys = list(data["value"].keys())
                    if keys:
                        null_key = self._rng.choice(keys)
                        data["value"][null_key] = None
            elif quality_issue == "malformed":
                data["value"] = {"_malformed": True, "_raw": json.dumps(data.get("value", {}))}

        elif behavior.behavior_type == BehaviorType.VOLUME:
            burst_factor = params.get("burst_factor", 1)
            data["headers"]["burst_factor"] = str(burst_factor)

        elif behavior.behavior_type == BehaviorType.ERROR_RATE:
            data["value"] = {
                "error": True,
                "error_type": params.get("error_type", "processing_error"),
                "error_message": self._faker.sentence(),
            }

        return data
