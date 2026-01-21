"""Main CLI entry point for the Persona Platform.

Provides deterministic dataset generation from the command line.
"""

from pathlib import Path
from typing import Any
import json
import sys

import click
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn

from persona_platform import __version__
from persona_platform.personas.loader import PersonaLoader, load_personas
from persona_platform.personas.registry import PersonaRegistry
from persona_platform.profiles.loader import ProfileLoader, load_profile
from persona_platform.generators.registry import GeneratorRegistry
from persona_platform.generators.base import DatasetType
from persona_platform.engine.persona_engine import PersonaEngine
from persona_platform.engine.validation_engine import ValidationEngine

console = Console()


@click.group()
@click.version_option(version=__version__, prog_name="persona-gen")
@click.option("--verbose", "-v", is_flag=True, help="Enable verbose output")
@click.pass_context
def cli(ctx: click.Context, verbose: bool) -> None:
    """Persona Platform - Generate test datasets from behavioral personas.

    A centralized, reusable platform that enables teams to generate many types
    of test datasets from shared behavioral personas.
    """
    ctx.ensure_object(dict)
    ctx.obj["verbose"] = verbose


@cli.command()
@click.argument("profile_path", type=click.Path(exists=True))
@click.option("--personas-dir", "-p", type=click.Path(exists=True), help="Directory containing persona files")
@click.option("--persona", multiple=True, help="Persona(s) to use (overrides profile)")
@click.option("--output", "-o", type=click.Path(), default="./output", help="Output directory")
@click.option("--format", "-f", type=click.Choice(["json", "jsonl", "csv"]), default="json", help="Output format")
@click.option("--seed", "-s", type=int, help="Random seed for reproducibility")
@click.option("--dry-run", is_flag=True, help="Show what would be generated without creating files")
@click.pass_context
def generate(
    ctx: click.Context,
    profile_path: str,
    personas_dir: str | None,
    persona: tuple[str, ...],
    output: str,
    format: str,
    seed: int | None,
    dry_run: bool,
) -> None:
    """Generate datasets from a profile.

    PROFILE_PATH is the path to the YAML profile file.
    """
    verbose = ctx.obj.get("verbose", False)

    try:
        profile = load_profile(profile_path)
        if seed is not None:
            profile.seed = seed

        # Override personas if specified on command line
        if persona:
            profile.personas = list(persona)

        registry = PersonaRegistry()
        if personas_dir:
            load_personas(personas_dir, registry)

        engine = PersonaEngine(persona_registry=registry)

        if dry_run:
            _show_dry_run(profile, engine)
            return

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:
            task = progress.add_task("Generating datasets...", total=None)

            result = engine.generate(profile)

            progress.update(task, description="Exporting files...")
            files = engine.export_result(result, output, format=format)

        console.print(Panel.fit(
            f"[green]Generated {result.total_records} records in {result.duration_seconds:.2f}s[/green]",
            title="Generation Complete",
        ))

        if verbose:
            console.print("\nCreated files:")
            for f in files:
                console.print(f"  - {f}")

    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        if verbose:
            import traceback
            console.print(traceback.format_exc())
        sys.exit(1)


@cli.command()
@click.argument("persona_name")
@click.option("--type", "-t", "dataset_type", type=click.Choice(["api", "streaming", "file", "load", "fault"]), default="api", help="Dataset type")
@click.option("--count", "-n", type=int, default=10, help="Number of records")
@click.option("--seed", "-s", type=int, help="Random seed")
@click.option("--personas-dir", "-p", type=click.Path(exists=True), help="Directory containing persona files")
@click.option("--output", "-o", type=click.Path(), help="Output file (stdout if not specified)")
@click.option("--pretty", is_flag=True, help="Pretty print JSON output")
@click.pass_context
def quick(
    ctx: click.Context,
    persona_name: str,
    dataset_type: str,
    count: int,
    seed: int | None,
    personas_dir: str | None,
    output: str | None,
    pretty: bool,
) -> None:
    """Quickly generate a dataset for a single persona.

    PERSONA_NAME is the name of the persona to use.
    """
    verbose = ctx.obj.get("verbose", False)

    try:
        registry = PersonaRegistry()
        if personas_dir:
            load_personas(personas_dir, registry)

        engine = PersonaEngine(persona_registry=registry)

        dataset = engine.generate_single(
            persona_name=persona_name,
            dataset_type=dataset_type,
            count=count,
            seed=seed,
        )

        if dataset is None:
            console.print(f"[red]Persona '{persona_name}' not found[/red]")
            sys.exit(1)

        records = [r.data for r in dataset.records]
        json_output = json.dumps(records, indent=2 if pretty else None, default=str)

        if output:
            Path(output).write_text(json_output)
            console.print(f"[green]Wrote {count} records to {output}[/green]")
        else:
            click.echo(json_output)

    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        if verbose:
            import traceback
            console.print(traceback.format_exc())
        sys.exit(1)


@cli.command()
@click.option("--personas-dir", "-p", type=click.Path(exists=True), help="Directory containing persona files")
@click.pass_context
def list_personas(ctx: click.Context, personas_dir: str | None) -> None:
    """List available personas."""
    registry = PersonaRegistry()

    if personas_dir:
        load_personas(personas_dir, registry)

    personas = list(registry)

    if not personas:
        console.print("[yellow]No personas found[/yellow]")
        return

    table = Table(title="Available Personas")
    table.add_column("Name", style="cyan")
    table.add_column("Version", style="green")
    table.add_column("Behaviors", justify="right")
    table.add_column("Tags")
    table.add_column("Description")

    for persona in personas:
        table.add_row(
            persona.name,
            persona.version,
            str(len(persona.behaviors)),
            ", ".join(persona.tags) if persona.tags else "-",
            persona.description[:50] + "..." if len(persona.description) > 50 else persona.description or "-",
        )

    console.print(table)


@cli.command()
@click.pass_context
def list_generators(ctx: click.Context) -> None:
    """List available dataset generators."""
    registry = GeneratorRegistry()

    table = Table(title="Available Generators")
    table.add_column("Type", style="cyan")
    table.add_column("Description")

    descriptions = {
        DatasetType.API: "API request/response data for contract testing",
        DatasetType.STREAMING: "Event streams for Kafka/Flink simulations",
        DatasetType.FILE: "File-based data for ETL validation",
        DatasetType.LOAD: "High-volume data for load testing",
        DatasetType.FAULT: "Faulty data for resilience testing",
    }

    for dtype in registry.list_types():
        table.add_row(dtype.value, descriptions.get(dtype, ""))

    console.print(table)


@cli.command()
@click.argument("path", type=click.Path(exists=True))
@click.option("--type", "-t", "item_type", type=click.Choice(["persona", "profile"]), default="persona", help="Type of file to validate")
@click.pass_context
def validate(ctx: click.Context, path: str, item_type: str) -> None:
    """Validate a persona or profile file.

    PATH is the path to the YAML file to validate.
    """
    validation_engine = ValidationEngine()

    try:
        if item_type == "persona":
            personas = load_personas(path, PersonaRegistry())
            for persona in personas:
                result = validation_engine.validate_persona(persona)
                _print_validation_result(persona.name, result)

        elif item_type == "profile":
            profile = load_profile(path)
            result = validation_engine.validate_profile(profile)
            _print_validation_result(profile.name, result)

    except Exception as e:
        console.print(f"[red]Error loading file: {e}[/red]")
        sys.exit(1)


@cli.command()
@click.option("--name", "-n", required=True, help="Profile name")
@click.option("--output", "-o", type=click.Path(), default="profile.yaml", help="Output file path")
@click.option("--personas", "-p", multiple=True, help="Persona names to include")
@click.option("--types", "-t", multiple=True, type=click.Choice(["api", "streaming", "file", "load", "fault"]), help="Dataset types to generate")
@click.pass_context
def init_profile(
    ctx: click.Context,
    name: str,
    output: str,
    personas: tuple[str, ...],
    types: tuple[str, ...],
) -> None:
    """Initialize a new profile file.

    Creates a template profile YAML file.
    """
    from persona_platform.profiles.base import Profile, DatasetConfig, OutputConfig
    from persona_platform.profiles.loader import ProfileLoader

    dataset_configs = []
    for t in types or ["api"]:
        dataset_configs.append(
            DatasetConfig(
                dataset_type=DatasetType(t),
                count=100,
            )
        )

    profile = Profile(
        name=name,
        description=f"Generated profile for {name}",
        personas=list(personas) if personas else ["example_persona"],
        datasets=dataset_configs,
        output=OutputConfig(),
    )

    loader = ProfileLoader()
    loader.save_file(profile, output)

    console.print(f"[green]Created profile: {output}[/green]")


@cli.command()
@click.option("--name", "-n", required=True, help="Persona name")
@click.option("--output", "-o", type=click.Path(), default="persona.yaml", help="Output file path")
@click.pass_context
def init_persona(ctx: click.Context, name: str, output: str) -> None:
    """Initialize a new persona file.

    Creates a template persona YAML file.
    """
    import yaml

    template = {
        "name": name,
        "version": "1.0.0",
        "description": f"Persona definition for {name}",
        "tags": ["example"],
        "behaviors": [
            {
                "name": "example_timing",
                "type": "timing",
                "description": "Example timing behavior",
                "parameters": {"pattern": "normal"},
                "weight": 1.0,
            },
            {
                "name": "example_error_rate",
                "type": "error_rate",
                "description": "Example error rate behavior",
                "parameters": {"rate": 0.01},
                "weight": 0.1,
            },
        ],
    }

    with open(output, "w") as f:
        yaml.dump(template, f, default_flow_style=False, sort_keys=False)

    console.print(f"[green]Created persona: {output}[/green]")


@cli.command()
@click.argument("schema_file", type=click.Path(exists=True))
@click.option("--name", "-n", help="Profile name (defaults to schema name)")
@click.option("--output", "-o", type=click.Path(), required=True, help="Output profile YAML path")
@click.option(
    "--dataset-type", "-t",
    type=click.Choice(["api", "streaming", "file", "load", "fault"]),
    default="api",
    help="Default dataset type"
)
@click.option("--count", "-c", type=int, default=100, help="Default record count")
@click.option("--output-format", type=click.Choice(["json", "jsonl", "csv"]), default="json", help="Output format")
@click.pass_context
def import_schema(
    ctx: click.Context,
    schema_file: str,
    name: str | None,
    output: str,
    dataset_type: str,
    count: int,
    output_format: str,
) -> None:
    """Import a canonical JSON schema and create a reusable profile.

    SCHEMA_FILE is the path to a canonical JSON schema file.

    \b
    The canonical format is the only supported import format. To convert from
    other formats (SQL, Avro, Protobuf, OpenAPI, etc.), use ChatGPT/Claude
    with the conversion prompts in docs/SCHEMA_IMPORT_FORMAT.md

    \b
    Canonical format example:
      {
        "name": "users",
        "fields": [
          {"name": "id", "type": "string", "format": "uuid", "required": true},
          {"name": "email", "type": "string", "format": "email"},
          {"name": "age", "type": "integer", "minimum": 0, "maximum": 150}
        ]
      }

    \b
    Examples:
      # Import a canonical JSON schema
      persona-gen import-schema users.import.json -o profiles/users.yaml

      # Import for streaming use case
      persona-gen import-schema events.json -t streaming -o profiles/events.yaml

      # Import with custom count
      persona-gen import-schema products.json -c 1000 -o profiles/products.yaml
    """
    import yaml
    from persona_platform.schemas import SchemaParser

    verbose = ctx.obj.get("verbose", False)

    try:
        # Create parser and parse the schema
        parser = SchemaParser.from_file(schema_file)
        schema_def = parser.parse()

        # Generate profile dict
        profile_name = name or f"{schema_def.name.lower().replace(' ', '_')}_profile"
        profile_dict = schema_def.to_profile_dict(
            dataset_type=dataset_type,
            count=count,
            output_format=output_format,
        )
        profile_dict["name"] = profile_name

        # Write profile YAML
        output_path = Path(output)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, "w") as f:
            # Add header comment
            f.write(f"# Auto-generated from: {schema_file}\n")
            f.write(f"# Schema: {schema_def.name}\n")
            f.write(f"# Generated by: persona-gen import-schema\n")
            f.write("#\n")
            f.write("# Usage:\n")
            f.write(f"#   persona-gen generate {output} -p /path/to/personas --persona <persona_name>\n")
            f.write("#\n\n")
            yaml.dump(profile_dict, f, default_flow_style=False, sort_keys=False, allow_unicode=True)

        console.print(Panel.fit(
            f"[green]Successfully imported schema![/green]\n\n"
            f"[cyan]Source:[/cyan] {schema_file}\n"
            f"[cyan]Schema:[/cyan] {schema_def.name}\n"
            f"[cyan]Fields:[/cyan] {len(schema_def.fields)}\n"
            f"[cyan]Required:[/cyan] {len(schema_def.get_required_fields())}\n"
            f"[cyan]Profile:[/cyan] {output}",
            title="Schema Import Complete",
        ))

        if verbose:
            console.print("\n[cyan]Fields:[/cyan]")
            for field in schema_def.fields:
                req = "[red]*[/red]" if field.required else ""
                fmt = f" ({field.format})" if field.format else ""
                console.print(f"  - {field.name}: {field.type}{fmt}{req}")

        console.print(f"\n[dim]To generate data, run:[/dim]")
        console.print(f"  [cyan]persona-gen generate {output} -p /path/to/personas --persona normal_user[/cyan]")

    except Exception as e:
        console.print(f"[red]Error importing schema: {e}[/red]")
        if verbose:
            import traceback
            console.print(traceback.format_exc())
        sys.exit(1)


def _show_dry_run(profile: Any, engine: PersonaEngine) -> None:
    """Show what would be generated in a dry run."""
    console.print(Panel.fit(
        f"Profile: [cyan]{profile.name}[/cyan]\n"
        f"Personas: {', '.join(profile.personas) or 'none'}\n"
        f"Seed: {profile.seed or 'random'}",
        title="Dry Run",
    ))

    table = Table(title="Datasets to Generate")
    table.add_column("Type", style="cyan")
    table.add_column("Count", justify="right")
    table.add_column("Enabled")

    for ds in profile.datasets:
        table.add_row(
            ds.dataset_type.value,
            str(ds.count),
            "[green]Yes[/green]" if ds.enabled else "[red]No[/red]",
        )

    console.print(table)


def _print_validation_result(name: str, result: Any) -> None:
    """Print validation results."""
    status = "[green]VALID[/green]" if result.valid else "[red]INVALID[/red]"
    console.print(f"\n{name}: {status}")

    if result.issues:
        for issue in result.issues:
            color = {
                "error": "red",
                "warning": "yellow",
                "info": "blue",
            }.get(issue.severity.value, "white")

            console.print(f"  [{color}]{issue.severity.value.upper()}[/{color}]: {issue.message}")
            if issue.path:
                console.print(f"    Path: {issue.path}")


if __name__ == "__main__":
    cli()
