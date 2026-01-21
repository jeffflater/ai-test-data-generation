"""Load Generator - Generates high-volume datasets for load testing.

Used for performance and soak testing.
"""

from typing import Any, Iterator
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass
import uuid
import math

from faker import Faker

from persona_platform.generators.base import (
    Generator,
    DatasetType,
    GeneratedDataset,
    GeneratedRecord,
)
from persona_platform.personas.base import Persona, Behavior, BehaviorType


@dataclass
class LoadProfile:
    """Defines the load pattern for generation."""

    name: str
    base_rate: float
    peak_rate: float
    ramp_up_seconds: float
    duration_seconds: float
    pattern: str = "constant"

    def get_rate_at(self, elapsed_seconds: float) -> float:
        """Calculate the request rate at a given time."""
        if self.pattern == "constant":
            return self.base_rate

        elif self.pattern == "ramp":
            if elapsed_seconds < self.ramp_up_seconds:
                progress = elapsed_seconds / self.ramp_up_seconds
                return self.base_rate + (self.peak_rate - self.base_rate) * progress
            return self.peak_rate

        elif self.pattern == "spike":
            spike_time = self.duration_seconds / 2
            spike_window = self.duration_seconds * 0.1
            if abs(elapsed_seconds - spike_time) < spike_window:
                return self.peak_rate
            return self.base_rate

        elif self.pattern == "wave":
            period = self.duration_seconds / 4
            amplitude = (self.peak_rate - self.base_rate) / 2
            offset = (self.peak_rate + self.base_rate) / 2
            return offset + amplitude * math.sin(2 * math.pi * elapsed_seconds / period)

        elif self.pattern == "step":
            steps = 5
            step_duration = self.duration_seconds / steps
            current_step = min(int(elapsed_seconds / step_duration), steps - 1)
            step_size = (self.peak_rate - self.base_rate) / (steps - 1)
            return self.base_rate + current_step * step_size

        return self.base_rate


class LoadGenerator(Generator):
    """Generator for load testing datasets.

    Produces high-volume data suitable for:
    - Performance testing
    - Soak testing
    - Stress testing
    - Capacity planning
    """

    dataset_type = DatasetType.LOAD

    LOAD_PROFILES = {
        "constant": LoadProfile("constant", 100, 100, 0, 60, "constant"),
        "ramp": LoadProfile("ramp", 10, 1000, 30, 120, "ramp"),
        "spike": LoadProfile("spike", 100, 5000, 0, 60, "spike"),
        "wave": LoadProfile("wave", 50, 500, 0, 120, "wave"),
        "step": LoadProfile("step", 10, 500, 0, 100, "step"),
        "soak": LoadProfile("soak", 200, 200, 10, 3600, "constant"),
    }

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
        """Generate load testing dataset from a persona.

        Options:
            load_profile: Name of load profile or LoadProfile instance
            request_type: Type of request (http, grpc, database, message)
            target_endpoint: Target endpoint or resource
            concurrent_users: Simulated concurrent users
            think_time_ms: Time between user actions
        """
        load_profile = options.get("load_profile", "constant")
        if isinstance(load_profile, str):
            load_profile = self.LOAD_PROFILES.get(load_profile, self.LOAD_PROFILES["constant"])

        request_type = options.get("request_type", "http")
        target_endpoint = options.get("target_endpoint", "/api/resource")
        concurrent_users = options.get("concurrent_users", 10)
        think_time_ms = options.get("think_time_ms", 1000)

        records = []
        start_time = datetime.now(timezone.utc)

        for i in range(count):
            elapsed_seconds = (i / max(count, 1)) * load_profile.duration_seconds
            current_rate = load_profile.get_rate_at(elapsed_seconds)

            base_data = self._generate_base_data(
                request_type=request_type,
                target_endpoint=target_endpoint,
                concurrent_users=concurrent_users,
                think_time_ms=think_time_ms,
                sequence=i,
                current_rate=current_rate,
                start_time=start_time,
                elapsed_seconds=elapsed_seconds,
            )

            data, applied = self._apply_all_behaviors(base_data, persona)

            records.append(
                GeneratedRecord(
                    data=data,
                    metadata={
                        "behaviors_applied": applied,
                        "persona": persona.name,
                        "generator": "load",
                        "load_profile": load_profile.name,
                        "current_rate": current_rate,
                    },
                    sequence_number=i,
                )
            )

        return GeneratedDataset(
            dataset_type=DatasetType.LOAD,
            records=records,
            persona_name=persona.name,
            seed=self._seed,
            total_count=count,
            metadata={
                "load_profile": load_profile.name,
                "request_type": request_type,
                "target_endpoint": target_endpoint,
                "concurrent_users": concurrent_users,
            },
        )

    def generate_load_plan(
        self,
        persona: Persona,
        profile: LoadProfile | str,
        time_resolution_seconds: float = 1.0,
    ) -> list[dict[str, Any]]:
        """Generate a load plan showing rate over time.

        Args:
            persona: The persona defining behaviors
            profile: Load profile to use
            time_resolution_seconds: Time granularity

        Returns:
            List of time points with target rates
        """
        if isinstance(profile, str):
            profile = self.LOAD_PROFILES.get(profile, self.LOAD_PROFILES["constant"])

        plan = []
        elapsed = 0.0

        while elapsed <= profile.duration_seconds:
            rate = profile.get_rate_at(elapsed)
            plan.append({
                "elapsed_seconds": elapsed,
                "target_rate": rate,
                "requests_in_interval": int(rate * time_resolution_seconds),
            })
            elapsed += time_resolution_seconds

        return plan

    def stream_load(
        self,
        persona: Persona,
        profile: LoadProfile | str,
        **options: Any,
    ) -> Iterator[tuple[float, GeneratedRecord]]:
        """Stream records according to a load profile.

        Yields:
            Tuples of (target_rate, record)
        """
        if isinstance(profile, str):
            profile = self.LOAD_PROFILES.get(profile, self.LOAD_PROFILES["constant"])

        sequence = 0
        elapsed = 0.0
        time_step = 0.1

        while elapsed <= profile.duration_seconds:
            rate = profile.get_rate_at(elapsed)
            requests_in_step = max(1, int(rate * time_step))

            for _ in range(requests_in_step):
                record = self.generate_single(
                    persona,
                    sequence_number=sequence,
                    elapsed_seconds=elapsed,
                    current_rate=rate,
                    **options,
                )
                yield (rate, record)
                sequence += 1

            elapsed += time_step

    def _generate_base_data(self, **options: Any) -> dict[str, Any]:
        """Generate base load test request data."""
        request_type = options.get("request_type", "http")
        target_endpoint = options.get("target_endpoint", "/api/resource")
        concurrent_users = options.get("concurrent_users", 10)
        think_time_ms = options.get("think_time_ms", 1000)
        sequence = options.get("sequence", 0)
        current_rate = options.get("current_rate", 100)
        start_time = options.get("start_time", datetime.now(timezone.utc))
        elapsed_seconds = options.get("elapsed_seconds", 0)

        request_timestamp = start_time + timedelta(seconds=elapsed_seconds)
        user_id = f"user_{sequence % concurrent_users}"
        session_id = f"session_{user_id}_{start_time.strftime('%Y%m%d%H%M%S')}"

        data = {
            "request_id": str(uuid.uuid4()),
            "request_type": request_type,
            "target": target_endpoint,
            "timestamp": request_timestamp.isoformat(),
            "sequence_number": sequence,
            "user_context": {
                "user_id": user_id,
                "session_id": session_id,
                "think_time_ms": think_time_ms,
            },
            "load_context": {
                "current_rate_rps": current_rate,
                "concurrent_users": concurrent_users,
                "elapsed_seconds": elapsed_seconds,
            },
            "request_payload": self._generate_request_payload(request_type),
            "expected_response": {
                "status": "success",
                "latency_p50_ms": 50,
                "latency_p95_ms": 200,
                "latency_p99_ms": 500,
            },
        }

        return data

    def _generate_request_payload(self, request_type: str) -> dict[str, Any]:
        """Generate request payload based on type."""
        if request_type == "http":
            return {
                "method": self._rng.choice(["GET", "POST", "PUT", "DELETE"]),
                "headers": {
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                },
                "body": {
                    "data": self._faker.text(max_nb_chars=200),
                },
            }
        elif request_type == "grpc":
            return {
                "service": f"com.example.{self._faker.word()}.Service",
                "method": f"Do{self._faker.word().title()}",
                "message": {
                    "id": str(uuid.uuid4()),
                    "payload": self._faker.text(max_nb_chars=100),
                },
            }
        elif request_type == "database":
            operations = ["SELECT", "INSERT", "UPDATE", "DELETE"]
            return {
                "operation": self._rng.choice(operations),
                "table": f"table_{self._faker.word()}",
                "conditions": {"id": self._rng.randint(1, 10000)},
            }
        elif request_type == "message":
            return {
                "topic": f"topic.{self._faker.word()}",
                "partition": self._rng.randint(0, 10),
                "message": {
                    "key": str(uuid.uuid4()),
                    "value": self._faker.text(max_nb_chars=500),
                },
            }
        return {"data": self._faker.text(max_nb_chars=100)}

    def _transform_for_behavior(
        self,
        data: dict[str, Any],
        behavior: Behavior,
    ) -> dict[str, Any]:
        """Apply behavior-specific transformations to load test data."""
        params = behavior.parameters

        if behavior.behavior_type == BehaviorType.LATENCY:
            base_latency = params.get("base_ms", 50)
            variance = params.get("variance_ms", 20)
            simulated_latency = max(1, self._rng.gauss(base_latency, variance))
            data["simulated_latency_ms"] = round(simulated_latency, 2)
            data["expected_response"]["latency_p50_ms"] = base_latency
            data["expected_response"]["latency_p95_ms"] = base_latency * 3
            data["expected_response"]["latency_p99_ms"] = base_latency * 6

        elif behavior.behavior_type == BehaviorType.ERROR_RATE:
            data["expected_response"]["status"] = "error"
            error_types = params.get("error_types", ["timeout", "server_error", "rate_limit"])
            data["expected_response"]["error_type"] = self._rng.choice(error_types)

        elif behavior.behavior_type == BehaviorType.VOLUME:
            burst_multiplier = params.get("burst_multiplier", 5)
            data["load_context"]["burst_mode"] = True
            data["load_context"]["current_rate_rps"] *= burst_multiplier

        elif behavior.behavior_type == BehaviorType.TIMING:
            pattern = params.get("pattern", "normal")
            if pattern == "slow_start":
                data["user_context"]["think_time_ms"] *= 3
            elif pattern == "aggressive":
                data["user_context"]["think_time_ms"] = max(10, data["user_context"]["think_time_ms"] // 10)

        elif behavior.behavior_type == BehaviorType.PATTERN:
            pattern_type = params.get("type", "normal")
            if pattern_type == "thundering_herd":
                data["load_context"]["thundering_herd"] = True
                data["user_context"]["think_time_ms"] = 0
            elif pattern_type == "cache_miss":
                data["request_payload"]["cache_bust"] = str(uuid.uuid4())

        return data
