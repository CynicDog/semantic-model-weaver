"""
Entry point: uv run python -m weaver [action] [options]

Actions
-------
  --setup                  Create workspace DB + schema + network policy. Safe to re-run.
  --reset-workspace        Drop and recreate the TruLens schema. Clears all evaluation
                           records. Requires confirmation unless --yes is passed.
  --database X --schema Y  Run the full pipeline against a Snowflake dataset (default).

Pipeline stages (implemented stages are wired; stubs are marked TODO):

  [SchemaDiscovery]    → schema_profile          (TODO: weaver/discovery.py)
  [YAMLWriter]         → semantic_model (YAML)   (TODO: weaver/writer.py)
  [ScenarioGenerator]  → golden_set, questions   (TODO: weaver/scenarios.py)
  [EvalLogger+TruLens] → records_df, feedback_df (weaver/evaluator.py)
  [RefinementAgent]    → refined semantic_model  (TODO: weaver/refiner.py)
"""

import logging
import os
import warnings

from dotenv import load_dotenv
from rich.console import Console

warnings.filterwarnings(
    "ignore",
    message="Bad owner or permissions on.*connections.toml",
    category=UserWarning,
)

console = Console()
log = logging.getLogger(__name__)

_DB = lambda: os.environ.get("WEAVER_SNOWFLAKE_DATABASE", "semantic_model_weaver")  # noqa: E731
_SCHEMA = lambda: os.environ.get("WEAVER_SNOWFLAKE_SCHEMA", "trulens")  # noqa: E731

_BANNER = """\
 Y8b Y8b Y888P  ,e e,   ,"Y88b Y8b Y888P  ,e e,  888,8,
  Y8b Y8b Y8P  d88 88b "8" 888  Y8b Y8P  d88 88b 888 "
   Y8b Y8b "   888   , ,ee 888   Y8b "   888   , 888
    YP  Y8P     "YeeP" "88 888    Y8P     "YeeP" 888    """


def _print_banner():
    console.print()
    console.print(_BANNER, style="bold cyan", highlight=False)
    console.print()
    console.print("  [bold white]Semantic Model Weaver[/bold white]  [dim]·  Snowflake Hackathon 2026 Seoul  ·  @CynicDog[/dim]")
    console.rule(style="cyan")
    console.print()


def _ok(msg: str):
    console.print(f"  [bold green]✓[/bold green]  {msg}")


def _info(msg: str):
    console.print(f"  [cyan]→[/cyan]  {msg}")


def _warn(msg: str):
    console.print(f"  [yellow]![/yellow]  {msg}")


def _session(with_database: bool = True):
    from snowflake.snowpark import Session

    cfg = {
        "account": os.environ["WEAVER_SNOWFLAKE_ACCOUNT"],
        "user": os.environ["WEAVER_SNOWFLAKE_USER"],
        "password": os.environ["WEAVER_SNOWFLAKE_PASSWORD"],
        "role": os.environ.get("WEAVER_SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
        "warehouse": os.environ.get("WEAVER_SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
    }
    if with_database:
        cfg["database"] = _DB()
        cfg["schema"] = _SCHEMA()
    return Session.builder.configs(cfg).create()


def _setup():
    db = _DB()
    schema = _SCHEMA()

    _info(f"connecting to Snowflake account [bold]{os.environ.get('WEAVER_SNOWFLAKE_ACCOUNT')}[/bold]")
    session = _session(with_database=False)

    session.sql(f"""
        CREATE DATABASE IF NOT EXISTS {db}
        COMMENT = 'Semantic Model Weaver — evaluation runs and TruLens records'
    """).collect()
    _ok(f"database [bold]{db}[/bold]")

    session.sql(f"""
        CREATE SCHEMA IF NOT EXISTS {db}.{schema}
        COMMENT = 'TruLens evaluation logs: records, feedback results'
    """).collect()
    _ok(f"schema [bold]{db}.{schema}[/bold]")

    role = os.environ.get("WEAVER_SNOWFLAKE_ROLE", "ACCOUNTADMIN")
    session.sql(f"""
        GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE {role}
    """).collect()
    _ok(f"Cortex usage granted to role [bold]{role}[/bold]")

    allowed_ip = os.environ.get("WEAVER_ALLOWED_IP")
    if allowed_ip:
        user = os.environ["WEAVER_SNOWFLAKE_USER"]
        _info("refreshing network policy …")
        session.sql(f"ALTER USER {user} UNSET NETWORK_POLICY").collect()
        session.sql("DROP NETWORK POLICY IF EXISTS weaver_dev").collect()
        session.sql(f"DROP NETWORK RULE IF EXISTS {db}.public.weaver_dev_ip").collect()
        session.sql(f"""
            CREATE NETWORK RULE {db}.public.weaver_dev_ip
                TYPE       = ipv4
                VALUE_LIST = ('{allowed_ip}')
                MODE       = ingress
                COMMENT    = 'Dev machine IP for Semantic Model Weaver local runs'
        """).collect()
        session.sql(f"""
            CREATE NETWORK POLICY weaver_dev
                ALLOWED_NETWORK_RULE_LIST = ('{db}.public.weaver_dev_ip')
                COMMENT = 'Network policy for Semantic Model Weaver development'
        """).collect()
        session.sql(f"ALTER USER {user} SET NETWORK_POLICY = weaver_dev").collect()
        _ok(f"network policy [bold]weaver_dev[/bold] → user [bold]{user}[/bold]  [dim](ip: {allowed_ip})[/dim]")
    else:
        _warn("WEAVER_ALLOWED_IP not set — skipping network policy")

    console.print()
    console.print("  [bold green]Setup complete.[/bold green]  Workspace is ready.")
    console.print()


def _reset_workspace(yes: bool):
    db = _DB()
    schema = _SCHEMA()

    if not yes:
        console.print(f"\n  [bold yellow]Warning:[/bold yellow] This will drop [bold]{db}.{schema}[/bold] and all TruLens records.")
        answer = console.input("  Type [bold]yes[/bold] to confirm: ")
        if answer.strip().lower() != "yes":
            _info("aborted")
            return

    _info(f"connecting to Snowflake …")
    session = _session(with_database=False)

    session.sql(f"DROP SCHEMA IF EXISTS {db}.{schema} CASCADE").collect()
    _ok(f"dropped [bold]{db}.{schema}[/bold]")

    session.sql(f"""
        CREATE SCHEMA {db}.{schema}
        COMMENT = 'TruLens evaluation logs: records, feedback results'
    """).collect()
    _ok(f"recreated [bold]{db}.{schema}[/bold]")

    console.print()
    console.print("  [bold green]Workspace reset.[/bold green]  All evaluation records cleared.")
    console.print()


def _run_dir(database: str, schema: str, timestamp: str) -> "pathlib.Path":
    import pathlib
    d = pathlib.Path("manifest") / f"{database}.{schema}" / timestamp
    d.mkdir(parents=True, exist_ok=True)
    return d


def _dump_yaml(run_dir, semantic_model, suffix: str = "") -> str:
    path = run_dir / f"model{suffix}.yaml"
    path.write_text(semantic_model.to_yaml(), encoding="utf-8")
    return str(path)


def _dump_scenarios(run_dir, golden_set: list, questions: list) -> str:
    import json
    path = run_dir / "scenarios.json"
    path.write_text(
        json.dumps({"golden_set": golden_set, "questions": questions}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return str(path)


def _dump_synonyms(run_dir, semantic_model, suffix: str = "") -> str:
    import json
    path = run_dir / f"synonyms{suffix}.json"
    data = {}
    for t in semantic_model.tables:
        cols = {}
        for col in [*t.dimensions, *t.time_dimensions, *t.measures]:
            if col.synonyms:
                cols[col.name] = col.synonyms
        if cols:
            data[t.name] = cols
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    return str(path)


def _run_pipeline(database: str, schema: str, iterations: int, version: str):
    import time as _time
    import datetime as _dt
    timestamp = _dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    version = f"{version}.{timestamp}"
    run_dir = _run_dir(database, schema, timestamp)
    _info(f"target: [bold]{database}.{schema}[/bold]  version: [bold]{version}[/bold]  iterations: [bold]{iterations}[/bold]")
    _info(f"manifest: [bold]{run_dir}[/bold]")
    session = _session(with_database=True)

    from weaver.discovery import SchemaDiscovery
    schema_profile = SchemaDiscovery(session).run(database, schema)
    _ok(f"discovered [bold]{len(schema_profile['tables'])}[/bold] tables in [bold]{database}.{schema}[/bold]")

    from weaver.writer import YAMLWriter
    semantic_model = YAMLWriter(session).generate(schema_profile)
    _ok(f"semantic model drafted — [bold]{len(semantic_model.tables)}[/bold] tables, [bold]{len(semantic_model.relationships)}[/bold] relationships")

    yaml_path = _dump_yaml(run_dir, semantic_model)
    _ok(f"YAML saved → [bold]{yaml_path}[/bold]")

    from weaver.enricher import SynonymEnricher
    semantic_model = SynonymEnricher(session).enrich(semantic_model)
    _ok(f"synonyms and descriptions enriched via Cortex")
    syn_path = _dump_synonyms(run_dir, semantic_model)
    _ok(f"synonyms saved → [bold]{syn_path}[/bold]")

    from weaver.scenarios import ScenarioGenerator
    golden_set, questions = ScenarioGenerator(session).generate(schema_profile)
    _ok(f"generated [bold]{len(questions)}[/bold] evaluation scenarios")
    scenarios_path = _dump_scenarios(run_dir, golden_set, questions)
    _ok(f"scenarios saved → [bold]{scenarios_path}[/bold]")

    from weaver.evaluator import (
        CortexAnalystApp,
        build_metrics,
        build_session,
        build_tru_app,
        get_results,
        run_evaluation,
    )

    tru_session, connector = build_session(session)
    metrics = build_metrics(session, golden_set)

    for iteration in range(1, iterations + 1):
        iter_version = f"{version}.iter{iteration}"
        console.rule(f"[cyan]iteration {iteration} / {iterations}  ·  {iter_version}[/cyan]")

        from weaver.probe import CortexAnalystProbe
        probe = CortexAnalystProbe(session, semantic_model.to_yaml())

        app = CortexAnalystApp(probe)
        tru_app = build_tru_app(app, connector=connector, version=iter_version)

        run_evaluation(tru_app, app, questions, metrics, version=iter_version)

        records_df, feedback_df = get_results(tru_session, snowpark_session=session, version=iter_version)
        mean_correctness = (
            feedback_df["correctness"].mean() if "correctness" in feedback_df.columns else 0.0
        )
        _ok(f"scored [bold]{len(records_df)}[/bold] questions  ·  mean correctness: [bold]{mean_correctness:.2f}[/bold]")

        from weaver.refiner import RefinementAgent
        patch = RefinementAgent(session).refine(semantic_model, feedback_df)
        if patch is None:
            _info("no further refinements needed — stopping early")
            break
        semantic_model = patch
        refined_path = _dump_yaml(run_dir, semantic_model, suffix=f".iter{iteration}")
        _ok(f"refined YAML saved → [bold]{refined_path}[/bold]")
        syn_path = _dump_synonyms(run_dir, semantic_model, suffix=f".iter{iteration}")
        _ok(f"refined synonyms saved → [bold]{syn_path}[/bold]")

    console.print()
    console.print("  [bold green]Done.[/bold green]  View results in [bold]Snowsight → AI & ML → Evaluations[/bold]")
    console.print()


def main():
    load_dotenv()

    # Silence noisy Snowflake connector / Snowpark INFO logs
    for noisy in ("snowflake.connector", "snowflake.snowpark"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")

    import argparse

    _print_banner()

    parser = argparse.ArgumentParser(
        prog="weaver",
        description="Semantic Model Weaver — Cortex Analyst semantic model generator and evaluator.",
    )
    parser.add_argument("--setup", action="store_true", help="Create workspace DB, schema, and network policy. Safe to re-run.")
    parser.add_argument("--reset-workspace", action="store_true", help="Drop and recreate the TruLens schema. Clears all evaluation records.")
    parser.add_argument("--yes", action="store_true", help="Skip confirmation prompt for --reset-workspace.")
    parser.add_argument("--database", help="Snowflake database to weave a model for (required for pipeline run).")
    parser.add_argument("--schema", help="Snowflake schema to weave a model for (required for pipeline run).")
    parser.add_argument("--iterations", type=int, default=3, help="Max refinement iterations (default: 3).")
    parser.add_argument("--version", default="v1", help="TruLens app version tag (default: v1).")
    args = parser.parse_args()

    if args.setup:
        _setup()
        return

    if args.reset_workspace:
        _reset_workspace(yes=args.yes)
        return

    if not args.database or not args.schema:
        parser.error("--database and --schema are required to run the pipeline (or use --setup / --reset-workspace)")

    _run_pipeline(args.database, args.schema, args.iterations, args.version)


if __name__ == "__main__":
    main()
