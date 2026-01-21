# Persona Platform

A centralized, reusable dataset persona platform that enables teams to generate many types of test datasets—API, streaming, files, load testing, and negative cases—from shared behavioral personas.

This approach transforms test data from static assets into declarative, behavior-driven infrastructure.

---

## Quick Start

### Build the Docker Image

```bash
docker-compose build
```

### List Available Personas

```bash
docker-compose run --rm persona-gen list-personas -p /app/personas
```

### List Available Generators

```bash
docker-compose run --rm persona-gen list-generators
```

---

## Docker Command Examples

### API Generator

Generate REST API request/response data for contract testing.

```bash
# Normal user - 10 API records
docker-compose run --rm persona-gen quick normal_user -t api -n 10 -p /app/personas --pretty

# Legacy client - 20 API records with compatibility issues
docker-compose run --rm persona-gen quick legacy_client -t api -n 20 -p /app/personas --pretty

# Edge case user - API data with boundary conditions
docker-compose run --rm persona-gen quick edge_case_user -t api -n 15 -p /app/personas --pretty

# High volume system - 100 API records
docker-compose run --rm persona-gen quick high_volume_system -t api -n 100 -p /app/personas

# Noisy device - API data with intermittent errors
docker-compose run --rm persona-gen quick noisy_device -t api -n 25 -p /app/personas --pretty
```

### Streaming Generator

Generate Kafka/Flink event streams for streaming simulations.

```bash
# Normal user - 50 streaming events
docker-compose run --rm persona-gen quick normal_user -t streaming -n 50 -p /app/personas --pretty

# Noisy device - streaming data with connectivity issues
docker-compose run --rm persona-gen quick noisy_device -t streaming -n 100 -p /app/personas --pretty

# High volume system - burst traffic simulation
docker-compose run --rm persona-gen quick high_volume_system -t streaming -n 500 -p /app/personas

# Legacy client - streaming with delayed transmission
docker-compose run --rm persona-gen quick legacy_client -t streaming -n 30 -p /app/personas --pretty

# Edge case user - rapid action events
docker-compose run --rm persona-gen quick edge_case_user -t streaming -n 75 -p /app/personas --pretty
```

### File Generator

Generate file-based data for ETL and ingestion validation.

```bash
# Normal user - 25 file records
docker-compose run --rm persona-gen quick normal_user -t file -n 25 -p /app/personas --pretty

# Legacy client - files with deprecated formats
docker-compose run --rm persona-gen quick legacy_client -t file -n 20 -p /app/personas --pretty

# High volume system - large batch file data
docker-compose run --rm persona-gen quick high_volume_system -t file -n 200 -p /app/personas

# Noisy device - file data with quality issues
docker-compose run --rm persona-gen quick noisy_device -t file -n 50 -p /app/personas --pretty

# Edge case user - files with unicode and boundary values
docker-compose run --rm persona-gen quick edge_case_user -t file -n 30 -p /app/personas --pretty
```

### Load Generator

Generate high-volume data for load and performance testing.

```bash
# High volume system - 1000 load test records
docker-compose run --rm persona-gen quick high_volume_system -t load -n 1000 -p /app/personas

# Normal user - 500 load test records
docker-compose run --rm persona-gen quick normal_user -t load -n 500 -p /app/personas

# Legacy client - load test with slow processing simulation
docker-compose run --rm persona-gen quick legacy_client -t load -n 300 -p /app/personas

# Noisy device - load test with intermittent failures
docker-compose run --rm persona-gen quick noisy_device -t load -n 400 -p /app/personas

# Edge case user - load test with concurrent requests
docker-compose run --rm persona-gen quick edge_case_user -t load -n 250 -p /app/personas
```

### Fault Generator

Generate faulty/mutated data for resilience and negative testing.

```bash
# Edge case user - 20 fault injection records (includes 1 valid baseline)
docker-compose run --rm persona-gen quick edge_case_user -t fault -n 20 -p /app/personas --pretty

# Normal user - fault data for validation testing
docker-compose run --rm persona-gen quick normal_user -t fault -n 15 -p /app/personas --pretty

# Legacy client - fault data with format issues
docker-compose run --rm persona-gen quick legacy_client -t fault -n 25 -p /app/personas --pretty

# Noisy device - fault data with transmission errors
docker-compose run --rm persona-gen quick noisy_device -t fault -n 30 -p /app/personas --pretty

# High volume system - fault data at scale
docker-compose run --rm persona-gen quick high_volume_system -t fault -n 50 -p /app/personas
```

---

## Output Options

### Save to File

```bash
# Save output to a JSON file
docker-compose run --rm persona-gen quick normal_user -t api -n 100 -p /app/personas -o /app/output/api_data.json

# Output will be available at ./output/api_data.json on your host machine
```

### Use a Seed for Reproducibility

```bash
# Same seed produces same output
docker-compose run --rm persona-gen quick normal_user -t api -n 10 -p /app/personas -s 42 --pretty
docker-compose run --rm persona-gen quick normal_user -t api -n 10 -p /app/personas -s 42 --pretty
```

---

## Schema Import (NEW)

Import external schema files to create reusable profiles. This allows you to generate test data that matches your existing database schemas, API specifications, or Kafka topics.

### Supported Schema Formats

| Format | File Extensions | Use Case |
|--------|-----------------|----------|
| **Swagger/OpenAPI** | `.json`, `.yaml` | REST API contracts |
| **JSON Schema** | `.json` | Data validation schemas |
| **Avro** | `.avsc` | Kafka topics, data pipelines |
| **SQL DDL** | `.sql` | Database tables |
| **Protobuf** | `.proto` | gRPC services, Kafka |

### Two-Step Workflow

**Step 1: Import schema → Create reusable profile (one-time)**

```bash
# Import from Swagger/OpenAPI
docker-compose run --rm persona-gen import-schema \
  /app/schemas/openapi-users.yaml \
  -e User \
  -o /app/profiles/user-api.yaml

# Import from SQL DDL
docker-compose run --rm persona-gen import-schema \
  /app/schemas/users.sql \
  -f ddl \
  -e users \
  -o /app/profiles/users-db.yaml

# Import from Avro (for Kafka streaming)
docker-compose run --rm persona-gen import-schema \
  /app/schemas/user-event.avsc \
  -t streaming \
  -o /app/profiles/user-events.yaml

# Import from JSON Schema
docker-compose run --rm persona-gen import-schema \
  /app/schemas/user.schema.json \
  -o /app/profiles/user-schema.yaml

# Import from Protobuf
docker-compose run --rm persona-gen import-schema \
  /app/schemas/user.proto \
  -f protobuf \
  -e User \
  -o /app/profiles/user-proto.yaml
```

**Step 2: Generate data using the profile (repeatable)**

```bash
# Generate with normal user behavior
docker-compose run --rm persona-gen generate /app/profiles/user-api.yaml \
  -p /app/personas \
  --persona normal_user

# Generate with edge case behavior
docker-compose run --rm persona-gen generate /app/profiles/user-api.yaml \
  -p /app/personas \
  --persona edge_case_user

# Generate fault injection data
docker-compose run --rm persona-gen generate /app/profiles/user-api.yaml \
  -p /app/personas \
  --persona noisy_device
```

### List Available Entities

Before importing, you can list all entities (schemas, tables, messages) in a file:

```bash
# List schemas in OpenAPI spec
docker-compose run --rm persona-gen import-schema \
  /app/schemas/openapi-users.yaml \
  --list-entities

# List tables in SQL DDL
docker-compose run --rm persona-gen import-schema \
  /app/schemas/users.sql \
  -f ddl \
  --list-entities

# List messages in Protobuf
docker-compose run --rm persona-gen import-schema \
  /app/schemas/user.proto \
  -f protobuf \
  --list-entities
```

### Import Options

```bash
persona-gen import-schema <SCHEMA_FILE> [OPTIONS]

Options:
  -f, --format          Schema format (auto-detected if not specified)
                        [swagger|openapi|jsonschema|avro|ddl|protobuf]
  -e, --entity          Entity to extract (schema/table/message name)
  -n, --name            Profile name (defaults to entity name)
  -o, --output          Output profile YAML path (required)
  -t, --dataset-type    Default dataset type [api|streaming|file|load|fault]
  -c, --count           Default record count (default: 100)
  --output-format       Output format [json|jsonl|csv]
  --list-entities       List available entities in the schema file
```

### Example: Complete Workflow

```bash
# 1. List available schemas in the OpenAPI spec
docker-compose run --rm persona-gen import-schema \
  /app/schemas/openapi-users.yaml --list-entities

# Output:
# Available Entities
# ┏━━━━━━━━━━━━━━━━━━━━┓
# ┃ Name               ┃
# ┡━━━━━━━━━━━━━━━━━━━━┩
# │ User               │
# │ Address            │
# │ CreateUserRequest  │
# │ Order              │
# │ OrderItem          │
# └────────────────────┘

# 2. Import the User schema
docker-compose run --rm persona-gen import-schema \
  /app/schemas/openapi-users.yaml \
  -e User \
  -o /app/profiles/user-profile.yaml \
  -c 500

# 3. Generate test data with different personas
docker-compose run --rm persona-gen generate /app/profiles/user-profile.yaml \
  -p /app/personas --persona normal_user -o /app/output

docker-compose run --rm persona-gen generate /app/profiles/user-profile.yaml \
  -p /app/personas --persona edge_case_user -o /app/output
```

---

## Profile-Based Generation

Generate datasets from a YAML profile configuration.

```bash
# Generate from API testing profile
docker-compose run --rm persona-gen generate /app/profiles/api_testing.yaml -p /app/personas -o /app/output

# Generate from streaming testing profile
docker-compose run --rm persona-gen generate /app/profiles/streaming_testing.yaml -p /app/personas -o /app/output

# Generate from load testing profile
docker-compose run --rm persona-gen generate /app/profiles/load_testing.yaml -p /app/personas -o /app/output

# Dry run - see what would be generated without creating files
docker-compose run --rm persona-gen generate /app/profiles/api_testing.yaml -p /app/personas --dry-run
```

---

## Validation Commands

```bash
# Validate a persona file
docker-compose run --rm persona-gen validate /app/personas/normal_user.yaml -t persona

# Validate a profile file
docker-compose run --rm persona-gen validate /app/profiles/api_testing.yaml -t profile
```

---

## Initialize New Personas/Profiles

```bash
# Create a new persona template
docker-compose run --rm persona-gen init-persona -n my_custom_persona -o /app/output/my_custom_persona.yaml

# Create a new profile template
docker-compose run --rm persona-gen init-profile -n my_project_profile -p normal_user -p noisy_device -t api -t streaming -o /app/output/my_profile.yaml
```

---

## Available Personas

| Persona | Description | Behaviors |
|---------|-------------|-----------|
| `normal_user` | Typical user with standard behavior patterns | timing, typos, session, rare errors |
| `noisy_device` | IoT device with connectivity and signal issues | intermittent connectivity, signal noise, delayed transmission, errors, battery drain |
| `legacy_client` | Older client with outdated protocols | slow processing, deprecated formats, connection issues, retry storms, limited features |
| `high_volume_system` | High-throughput system with burst patterns | burst traffic, sustained load, minimal latency, batch operations, rate limits |
| `edge_case_user` | User testing boundary conditions | boundary values, unicode, rapid actions, concurrent requests, malformed inputs, session anomalies |

## Available Generators

| Generator | Description | Use Case |
|-----------|-------------|----------|
| `api` | REST API request/response data | Contract testing, integration testing |
| `streaming` | Kafka/Flink event streams | Stream processing validation |
| `file` | File-based data records | ETL and ingestion testing |
| `load` | High-volume load test data | Performance and soak testing |
| `fault` | Faulty/mutated data | Resilience and negative testing |

---

## Design Summary

### Core Design Principles

| Principle | Summary |
|-----------|---------|
| Behavior over data | Personas describe how systems behave, not exact values |
| Generate, don't store | Datasets are produced on demand from generators |
| Central logic, local intent | Shared generators with project-specific profiles |
| Deterministic & repeatable | Seeded generation ensures stability |
| Composable & scalable | Personas combine without duplication |
| AI-accelerated, human-governed | Copilot assists; humans decide realism |

### Key Architectural Layers

#### 1. Personas (Behavior Layer)

Declarative definitions of real-world behavior, such as:
- Noisy devices
- Legacy clients
- Edge-case users

**Characteristics:**
- Dataset-type agnostic
- Composable and optionally inheritable
- Versioned and centrally owned

**Role:** Define realism once and reuse it everywhere.

#### 2. Generators (Shape Layer)

**Responsibilities:**
- Convert persona behavior into concrete datasets
- Support dataset-specific formats: API, Streaming, File-based, Load/scale, Mutation/fault injection

**Constraints:**
- Never redefine behavior
- Strict separation from personas

**Role:** Translate behavior into data shapes.

#### 3. Profiles (Binding Layer)

Project-owned configuration that specifies:
- Selected personas
- Dataset types
- Scale and volume
- Randomization seeds

**Constraints:**
- No generation logic
- No behavioral definitions

**Role:** Declare project-specific intent.

#### 4. Runtime & Tooling

**Platform services:**
- Persona engine applies behavioral rules
- Validation engine enforces schema correctness
- CLI provides deterministic dataset generation

**Role:** Enable safe, repeatable execution across environments.

### Multi-Dataset Output Model

From the same persona set, the platform can generate multiple dataset shapes:

| Dataset Type | Example Usage |
|--------------|---------------|
| API CRUD | Contract and integration testing |
| Streaming | Kafka / Flink simulations |
| Files | Ingest and ETL validation |
| Load | Performance and soak testing |
| Faulty data | Negative and resilience testing |

**Result:** One realism model → many dataset shapes

### Business Value

- Massive reduction in manual test-data effort
- Consistent realism across teams and regions
- Faster onboarding for new projects
- Lower maintenance cost over time
- Clear visibility into tested behaviors

---

## Final Takeaway

This design establishes a shared "data realism engine" that scales across projects, teams, and dataset types. Personas ensure realism remains intentional, repeatable, and maintainable.
