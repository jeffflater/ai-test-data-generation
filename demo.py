#!/usr/bin/env python3
"""
Demo script showing basic usage of the Persona Platform.

Run this script after installing the package:
    pip install -e .
    python demo.py
"""

from persona_platform.personas.base import Persona, Behavior, BehaviorType, PersonaBuilder
from persona_platform.personas.registry import PersonaRegistry
from persona_platform.generators.api_generator import APIGenerator
from persona_platform.generators.streaming_generator import StreamingGenerator
from persona_platform.generators.fault_generator import FaultGenerator
from persona_platform.profiles.base import Profile, ProfileBuilder, DatasetConfig
from persona_platform.generators.base import DatasetType
from persona_platform.engine.persona_engine import PersonaEngine
from persona_platform.engine.validation_engine import ValidationEngine


def demo_persona_creation():
    """Demonstrate creating personas programmatically."""
    print("=" * 60)
    print("1. CREATING PERSONAS")
    print("=" * 60)

    noisy_device = (
        PersonaBuilder("noisy_device")
        .version("1.0.0")
        .description("A device with intermittent connectivity")
        .tag("device", "iot")
        .behavior(
            name="signal_noise",
            behavior_type=BehaviorType.DATA_QUALITY,
            parameters={"noise_factor": 0.2},
            weight=0.3,
        )
        .behavior(
            name="delayed_transmission",
            behavior_type=BehaviorType.LATENCY,
            parameters={"delay_ms": 2000},
            weight=0.25,
        )
        .build()
    )

    print(f"Created persona: {noisy_device.name}")
    print(f"  Version: {noisy_device.version}")
    print(f"  Tags: {noisy_device.tags}")
    print(f"  Behaviors: {[b.name for b in noisy_device.behaviors]}")
    print()

    return noisy_device


def demo_api_generation(persona):
    """Demonstrate API data generation."""
    print("=" * 60)
    print("2. GENERATING API DATA")
    print("=" * 60)

    generator = APIGenerator(seed=42)

    dataset = generator.generate(
        persona=persona,
        count=3,
        method="POST",
        endpoint="/api/v1/devices",
        schema={
            "device_id": "uuid",
            "temperature": "float",
            "status": "string",
        },
    )

    print(f"Generated {len(dataset)} API records")
    print(f"Dataset type: {dataset.dataset_type.value}")
    print()

    for i, record in enumerate(dataset.records[:2]):
        print(f"Record {i + 1}:")
        print(f"  Request method: {record.data['request']['method']}")
        print(f"  Request body: {record.data['request']['body']}")
        if 'response' in record.data:
            print(f"  Response status: {record.data['response']['status_code']}")
        print()


def demo_streaming_generation(persona):
    """Demonstrate streaming event generation."""
    print("=" * 60)
    print("3. GENERATING STREAMING EVENTS")
    print("=" * 60)

    generator = StreamingGenerator(seed=42)

    dataset = generator.generate(
        persona=persona,
        count=5,
        topic="device-events",
        event_type="sensor_reading",
    )

    print(f"Generated {len(dataset)} streaming events")
    print()

    for i, record in enumerate(dataset.records[:2]):
        print(f"Event {i + 1}:")
        print(f"  Topic: {record.data['topic']}")
        print(f"  Partition: {record.data['partition']}")
        print(f"  Timestamp: {record.data['timestamp']}")
        print(f"  Value: {record.data['value']}")
        print()


def demo_fault_injection(persona):
    """Demonstrate fault injection for resilience testing."""
    print("=" * 60)
    print("4. GENERATING FAULTY DATA")
    print("=" * 60)

    generator = FaultGenerator(seed=42)

    dataset = generator.generate(
        persona=persona,
        count=3,
        fault_probability=0.8,
        include_valid_baseline=True,
    )

    print(f"Generated {len(dataset)} records (including baseline)")
    print()

    for i, record in enumerate(dataset.records[:3]):
        record_type = record.data.get("type", "unknown")
        print(f"Record {i + 1} ({record_type}):")
        if record_type == "valid_baseline":
            print("  This is the valid baseline for comparison")
        else:
            faults = record.data.get("faults_injected", [])
            print(f"  Faults injected: {len(faults)}")
            for fault in faults[:2]:
                print(f"    - {fault.get('fault_type')}: {fault.get('description', 'N/A')}")
        print()


def demo_engine_generation():
    """Demonstrate using the PersonaEngine with profiles."""
    print("=" * 60)
    print("5. ENGINE-BASED GENERATION")
    print("=" * 60)

    registry = PersonaRegistry()

    registry.register(
        PersonaBuilder("normal_user")
        .behavior("standard_timing", BehaviorType.TIMING, {"pattern": "normal"}, 0.9)
        .build()
    )

    registry.register(
        PersonaBuilder("error_prone")
        .behavior("high_errors", BehaviorType.ERROR_RATE, {"rate": 0.3}, 0.8)
        .build()
    )

    profile = (
        ProfileBuilder("demo_profile")
        .description("Demo generation profile")
        .persona("normal_user", "error_prone")
        .dataset(DatasetType.API, count=5)
        .seed(12345)
        .build()
    )

    engine = PersonaEngine(persona_registry=registry)
    result = engine.generate(profile)

    print(f"Profile: {result.profile.name}")
    print(f"Total records: {result.total_records}")
    print(f"Duration: {result.duration_seconds:.3f}s")
    print(f"Datasets generated: {len(result.datasets)}")
    print()

    for key, dataset in result.datasets.items():
        print(f"  {key}: {len(dataset)} records")


def demo_validation():
    """Demonstrate validation capabilities."""
    print("=" * 60)
    print("6. VALIDATION")
    print("=" * 60)

    validation_engine = ValidationEngine()

    valid_persona = Persona(
        name="valid_persona",
        behaviors=[
            Behavior(name="b1", behavior_type=BehaviorType.TIMING, weight=0.5),
        ],
    )

    invalid_persona = Persona(
        name="",
        behaviors=[
            Behavior(name="dup", behavior_type=BehaviorType.TIMING),
            Behavior(name="dup", behavior_type=BehaviorType.LATENCY),
        ],
    )

    result1 = validation_engine.validate_persona(valid_persona)
    print(f"Valid persona: {'PASS' if result1.valid else 'FAIL'}")

    result2 = validation_engine.validate_persona(invalid_persona)
    print(f"Invalid persona: {'PASS' if result2.valid else 'FAIL'}")
    for issue in result2.issues:
        print(f"  - [{issue.severity.value}] {issue.message}")


def main():
    """Run all demos."""
    print()
    print("PERSONA PLATFORM DEMO")
    print("=" * 60)
    print()

    persona = demo_persona_creation()
    demo_api_generation(persona)
    demo_streaming_generation(persona)
    demo_fault_injection(persona)
    demo_engine_generation()
    demo_validation()

    print()
    print("=" * 60)
    print("DEMO COMPLETE")
    print("=" * 60)
    print()
    print("To use the CLI, install the package and run:")
    print("  pip install -e .")
    print("  persona-gen --help")
    print()


if __name__ == "__main__":
    main()
