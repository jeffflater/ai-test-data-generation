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

## Schema Import

Import schemas to create reusable profiles for test data generation. This allows you to generate test data that matches your existing database schemas, API specifications, or Kafka topics.

### Universal Format Support via LLM Conversion

Instead of maintaining parsers for every schema format, we use a **canonical JSON format** that you can convert to from ANY source using ChatGPT or Claude. This approach:

- Supports unlimited formats (SQL, Avro, Protobuf, OpenAPI, GraphQL, TypeScript, etc.)
- Requires zero maintenance for evolving external specifications
- Leverages LLMs which excel at schema translation

### Three-Step Workflow

**Step 1: Convert your schema to canonical JSON format (one-time)**

Copy this universal prompt into ChatGPT/Claude, then paste your schema at the end:

<details>
<summary><strong>Click to expand Universal Conversion Prompt</strong></summary>

```
Convert the following schema to the canonical JSON format for test data generation.

**Output Format:**
{
  "name": "<entity_name>",
  "description": "<optional description>",
  "fields": [
    {
      "name": "<field_name>",
      "type": "<string|integer|float|boolean|array|object>",
      "format": "<optional: uuid|email|date|datetime|time|url|hostname|ipv4|ipv6|phone>",
      "required": <true|false>,
      "nullable": <true|false>,
      "enum": ["<value1>", "<value2>"],
      "min_length": <number>,
      "max_length": <number>,
      "minimum": <number>,
      "maximum": <number>,
      "pattern": "<regex>",
      "default": <value>,
      "items": { <nested field schema for arrays> },
      "properties": { "<name>": <nested field schema for objects> }
    }
  ]
}

**Conversion Rules:**
1. Identify the entity/table/message name → use as "name"
2. Map each field/column/property to the fields array
3. Map types to: string, integer, float, boolean, array, object
4. Detect formats from type names or field names:
   - UUID/GUID → format: "uuid"
   - Fields named *email* → format: "email"
   - Fields named *phone* → format: "phone"
   - Fields named *url*/*uri* → format: "url"
   - DATE → format: "date"
   - DATETIME/TIMESTAMP → format: "datetime"
5. NOT NULL or required markers → required: true, nullable: false
6. PRIMARY KEY → required: true, nullable: false
7. String length limits → max_length
8. Numeric ranges or CHECK constraints → minimum/maximum
9. ENUM types or CHECK IN (...) → enum array
10. DEFAULT values → default property
11. Arrays/repeated fields → type: "array" with items schema
12. Nested objects/records → type: "object" with properties

**Important:**
- Output ONLY valid JSON, no markdown code blocks, no explanation
- Include only properties that have values (omit null/empty properties)
- For nested objects and arrays, recursively apply the same field schema format

**Schema to convert:**
[PASTE YOUR SCHEMA HERE]
```

</details>

**Example conversion:**

```
Input (SQL DDL):
  CREATE TABLE users (
    id UUID PRIMARY KEY,
    email VARCHAR(255) NOT NULL,
    age INTEGER CHECK (age >= 0 AND age <= 150)
  );

Output (Canonical JSON):
  {
    "name": "users",
    "fields": [
      {"name": "id", "type": "string", "format": "uuid", "required": true},
      {"name": "email", "type": "string", "format": "email", "required": true, "max_length": 255},
      {"name": "age", "type": "integer", "minimum": 0, "maximum": 150}
    ]
  }
```

Save the JSON output as a `.json` file (e.g., `users.json`).

**Step 2: Import the canonical JSON → Create reusable profile**

```bash
# Import for API testing
docker-compose run --rm persona-gen import-schema \
  /app/schemas/users.json \
  -o /app/profiles/users.yaml

# Import for streaming with higher count
docker-compose run --rm persona-gen import-schema \
  /app/schemas/events.json \
  -t streaming \
  -c 1000 \
  -o /app/profiles/events.yaml

# Import for load testing
docker-compose run --rm persona-gen import-schema \
  /app/schemas/orders.json \
  -t load \
  -c 10000 \
  -o /app/profiles/orders-load.yaml
```

**Step 3: Generate data using the profile (repeatable)**

```bash
# Generate with normal user behavior
docker-compose run --rm persona-gen generate /app/profiles/users.yaml \
  -p /app/personas \
  --persona normal_user

# Generate with edge case behavior
docker-compose run --rm persona-gen generate /app/profiles/users.yaml \
  -p /app/personas \
  --persona edge_case_user

# Generate fault injection data
docker-compose run --rm persona-gen generate /app/profiles/users.yaml \
  -p /app/personas \
  --persona noisy_device
```

### Canonical JSON Format

The canonical format supports all common schema features:

```json
{
  "name": "users",
  "description": "User accounts",
  "fields": [
    {
      "name": "id",
      "type": "string",
      "format": "uuid",
      "required": true
    },
    {
      "name": "email",
      "type": "string",
      "format": "email",
      "required": true,
      "max_length": 255
    },
    {
      "name": "age",
      "type": "integer",
      "minimum": 0,
      "maximum": 150
    },
    {
      "name": "role",
      "type": "string",
      "enum": ["admin", "user", "guest"],
      "default": "user"
    },
    {
      "name": "tags",
      "type": "array",
      "items": {"name": "tag", "type": "string"},
      "unique_items": true,
      "max_items": 10
    },
    {
      "name": "preferences",
      "type": "object",
      "properties": {
        "theme": {"name": "theme", "type": "string", "enum": ["light", "dark"]},
        "notifications": {"name": "notifications", "type": "boolean", "default": true}
      }
    }
  ]
}
```

**Supported types:** `string`, `integer`, `float`, `boolean`, `array`, `object`

**Supported formats:** `uuid`, `email`, `date`, `datetime`, `time`, `url`, `hostname`, `ipv4`, `ipv6`, `phone`

**Full specification:** See `docs/SCHEMA_IMPORT_FORMAT.md`

### Import Options

```bash
persona-gen import-schema <SCHEMA_FILE> [OPTIONS]

Options:
  -n, --name            Profile name (defaults to schema name)
  -o, --output          Output profile YAML path (required)
  -t, --dataset-type    Default dataset type [api|streaming|file|load|fault]
  -c, --count           Default record count (default: 100)
  --output-format       Output format [json|jsonl|csv]
```

### Conversion Prompts

Ready-to-use prompts for converting from common formats are in `docs/SCHEMA_IMPORT_FORMAT.md`:

| Source Format | Prompt Available |
|---------------|------------------|
| SQL DDL | Yes |
| Apache Avro | Yes |
| Protocol Buffers | Yes |
| OpenAPI/Swagger | Yes |
| GraphQL | Yes |
| TypeScript | Yes |

### Example: Complete Workflow

```bash
# 1. Convert your SQL DDL using ChatGPT with the prompt from docs/SCHEMA_IMPORT_FORMAT.md
#    Save the output as users.json

# 2. Import the canonical schema
docker-compose run --rm persona-gen import-schema \
  /app/schemas/users.json \
  -o /app/profiles/users.yaml \
  -c 500

# 3. Generate test data with different personas
docker-compose run --rm persona-gen generate /app/profiles/users.yaml \
  -p /app/personas --persona normal_user -o /app/output

docker-compose run --rm persona-gen generate /app/profiles/users.yaml \
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
