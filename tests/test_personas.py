"""Tests for the Personas module."""

import pytest
from persona_platform.personas.base import (
    Persona,
    Behavior,
    BehaviorType,
    PersonaBuilder,
)
from persona_platform.personas.registry import PersonaRegistry


class TestBehavior:
    """Tests for Behavior class."""

    def test_create_behavior(self):
        behavior = Behavior(
            name="test_behavior",
            behavior_type=BehaviorType.TIMING,
            description="A test behavior",
            parameters={"delay_ms": 100},
            weight=0.5,
        )

        assert behavior.name == "test_behavior"
        assert behavior.behavior_type == BehaviorType.TIMING
        assert behavior.weight == 0.5
        assert behavior.parameters["delay_ms"] == 100

    def test_behavior_weight_bounds(self):
        with pytest.raises(ValueError):
            Behavior(
                name="invalid",
                behavior_type=BehaviorType.ERROR_RATE,
                weight=1.5,
            )

        with pytest.raises(ValueError):
            Behavior(
                name="invalid",
                behavior_type=BehaviorType.ERROR_RATE,
                weight=-0.1,
            )


class TestPersona:
    """Tests for Persona class."""

    def test_create_persona(self):
        persona = Persona(
            name="test_persona",
            version="1.0.0",
            description="A test persona",
            tags=["test", "example"],
        )

        assert persona.name == "test_persona"
        assert persona.version == "1.0.0"
        assert "test" in persona.tags

    def test_add_behavior(self):
        persona = Persona(name="test")
        behavior = Behavior(
            name="test_behavior",
            behavior_type=BehaviorType.LATENCY,
        )

        persona.add_behavior(behavior)
        assert len(persona.behaviors) == 1
        assert persona.behaviors[0].name == "test_behavior"

    def test_get_behaviors_by_type(self):
        persona = Persona(
            name="test",
            behaviors=[
                Behavior(name="timing1", behavior_type=BehaviorType.TIMING),
                Behavior(name="latency1", behavior_type=BehaviorType.LATENCY),
                Behavior(name="timing2", behavior_type=BehaviorType.TIMING),
            ],
        )

        timing_behaviors = persona.get_behaviors_by_type(BehaviorType.TIMING)
        assert len(timing_behaviors) == 2

    def test_merge_personas(self):
        persona1 = Persona(
            name="persona1",
            behaviors=[
                Behavior(name="behavior1", behavior_type=BehaviorType.TIMING),
            ],
            tags=["tag1"],
        )
        persona2 = Persona(
            name="persona2",
            behaviors=[
                Behavior(name="behavior2", behavior_type=BehaviorType.LATENCY),
            ],
            tags=["tag2"],
        )

        merged = persona1.merge_with(persona2)

        assert "persona1" in merged.name
        assert "persona2" in merged.name
        assert len(merged.behaviors) == 2
        assert set(merged.tags) == {"tag1", "tag2"}


class TestPersonaBuilder:
    """Tests for PersonaBuilder."""

    def test_builder_creates_persona(self):
        persona = (
            PersonaBuilder("test_persona")
            .version("2.0.0")
            .description("Built persona")
            .tag("test", "builder")
            .behavior(
                name="test_behavior",
                behavior_type=BehaviorType.ERROR_RATE,
                parameters={"rate": 0.1},
                weight=0.5,
            )
            .build()
        )

        assert persona.name == "test_persona"
        assert persona.version == "2.0.0"
        assert len(persona.behaviors) == 1
        assert len(persona.tags) == 2


class TestPersonaRegistry:
    """Tests for PersonaRegistry."""

    def test_register_and_get(self):
        registry = PersonaRegistry()
        persona = Persona(name="test", version="1.0.0")

        registry.register(persona)
        retrieved = registry.get("test")

        assert retrieved is not None
        assert retrieved.name == "test"

    def test_get_latest_version(self):
        registry = PersonaRegistry()
        registry.register(Persona(name="test", version="1.0.0"))
        registry.register(Persona(name="test", version="2.0.0"))

        latest = registry.get("test")
        assert latest is not None
        assert latest.version == "2.0.0"

    def test_get_specific_version(self):
        registry = PersonaRegistry()
        registry.register(Persona(name="test", version="1.0.0"))
        registry.register(Persona(name="test", version="2.0.0"))

        v1 = registry.get("test", "1.0.0")
        assert v1 is not None
        assert v1.version == "1.0.0"

    def test_duplicate_registration_raises(self):
        registry = PersonaRegistry()
        persona = Persona(name="test", version="1.0.0")

        registry.register(persona)

        with pytest.raises(ValueError):
            registry.register(persona)

    def test_list_all(self):
        registry = PersonaRegistry()
        registry.register(Persona(name="test1"))
        registry.register(Persona(name="test2"))

        names = registry.list_all()
        assert "test1" in names
        assert "test2" in names

    def test_unregister(self):
        registry = PersonaRegistry()
        registry.register(Persona(name="test"))

        assert registry.unregister("test")
        assert registry.get("test") is None
        assert not registry.unregister("nonexistent")

    def test_contains(self):
        registry = PersonaRegistry()
        registry.register(Persona(name="test"))

        assert "test" in registry
        assert "nonexistent" not in registry

    def test_resolve_inheritance(self):
        registry = PersonaRegistry()

        parent = Persona(
            name="parent",
            behaviors=[
                Behavior(name="parent_behavior", behavior_type=BehaviorType.TIMING),
            ],
        )
        child = Persona(
            name="child",
            inherits_from=["parent"],
            behaviors=[
                Behavior(name="child_behavior", behavior_type=BehaviorType.LATENCY),
            ],
        )

        registry.register(parent)
        registry.register(child)

        resolved = registry.get_resolved("child")
        assert resolved is not None
        assert len(resolved.behaviors) == 2
