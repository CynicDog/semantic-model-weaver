"""
Entry point: uv run python -m forge [action] [options]

Actions
-------
  --setup                  Create workspace DB + schema + network policy. Safe to re-run.
  --reset-workspace        Drop and recreate the TruLens schema. Clears all evaluation
                           records. Requires confirmation unless --yes is passed.
  --database X --schema Y  Run the full pipeline against a Snowflake dataset (default).

Pipeline stages (implemented stages are wired; stubs are marked TODO):

  [SchemaDiscovery]    → schema_profile          (TODO: forge/discovery.py)
  [YAMLWriter]         → semantic_model (YAML)   (TODO: forge/writer.py)
  [ScenarioGenerator]  → golden_set, questions   (TODO: forge/scenarios.py)
  [EvalLogger+TruLens] → records_df, feedback_df (forge/evaluator.py)
  [RefinementAgent]    → refined semantic_model  (TODO: forge/refiner.py)
"""

import logging
import os
import sys
import warnings

from dotenv import load_dotenv

# Snowflake connector emits a UserWarning about ~/.snowflake/connections.toml
# permissions on every connection. It's cosmetic and doesn't affect functionality.
warnings.filterwarnings(
    "ignore",
    message="Bad owner or permissions on.*connections.toml",
    category=UserWarning,
)

log = logging.getLogger(__name__)

_DB = lambda: os.environ.get("FORGE_SNOWFLAKE_DATABASE", "semantic_model_forge")  # noqa: E731
_SCHEMA = lambda: os.environ.get("FORGE_SNOWFLAKE_SCHEMA", "trulens")  # noqa: E731


def _session(with_database: bool = True):
    from snowflake.snowpark import Session

    cfg = {
        "account": os.environ["FORGE_SNOWFLAKE_ACCOUNT"],
        "user": os.environ["FORGE_SNOWFLAKE_USER"],
        "password": os.environ["FORGE_SNOWFLAKE_PASSWORD"],
        "role": os.environ.get("FORGE_SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
        "warehouse": os.environ.get("FORGE_SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
    }
    if with_database:
        cfg["database"] = _DB()
        cfg["schema"] = _SCHEMA()
    return Session.builder.configs(cfg).create()


def _setup():
    db = _DB()
    schema = _SCHEMA()
    session = _session(with_database=False)

    session.sql(f"""
        CREATE DATABASE IF NOT EXISTS {db}
        COMMENT = 'Semantic Model Forge — evaluation runs and TruLens records'
    """).collect()
    log.info("database %s: ready", db)

    session.sql(f"""
        CREATE SCHEMA IF NOT EXISTS {db}.{schema}
        COMMENT = 'TruLens evaluation logs: records, feedback results'
    """).collect()
    log.info("schema %s.%s: ready", db, schema)

    allowed_ip = os.environ.get("FORGE_ALLOWED_IP")
    if allowed_ip:
        user = os.environ["FORGE_SNOWFLAKE_USER"]
        # Tear down in dependency order before recreating
        session.sql(f"ALTER USER {user} UNSET NETWORK_POLICY").collect()
        session.sql("DROP NETWORK POLICY IF EXISTS forge_dev").collect()
        session.sql(f"DROP NETWORK RULE IF EXISTS {db}.public.forge_dev_ip").collect()
        session.sql(f"""
            CREATE NETWORK RULE {db}.public.forge_dev_ip
                TYPE       = ipv4
                VALUE_LIST = ('{allowed_ip}')
                MODE       = ingress
                COMMENT    = 'Dev machine IP for Semantic Model Forge local runs'
        """).collect()
        session.sql(f"""
            CREATE NETWORK POLICY forge_dev
                ALLOWED_NETWORK_RULE_LIST = ('{db}.public.forge_dev_ip')
                COMMENT = 'Network policy for Semantic Model Forge development'
        """).collect()
        session.sql(f"ALTER USER {user} SET NETWORK_POLICY = forge_dev").collect()
        log.info("network policy forge_dev set for user %s (ip: %s)", user, allowed_ip)
    else:
        log.info("FORGE_ALLOWED_IP not set — skipping network policy")

    log.info("setup complete")


def _reset_workspace(yes: bool):
    db = _DB()
    schema = _SCHEMA()

    if not yes:
        answer = input(
            f"Drop {db}.{schema} and all TruLens records? This cannot be undone. "
            "Type 'yes' to confirm: "
        )
        if answer.strip().lower() != "yes":
            log.info("aborted")
            return

    session = _session(with_database=False)
    session.sql(f"DROP SCHEMA IF EXISTS {db}.{schema} CASCADE").collect()
    log.info("dropped %s.%s", db, schema)

    session.sql(f"""
        CREATE SCHEMA {db}.{schema}
        COMMENT = 'TruLens evaluation logs: records, feedback results'
    """).collect()
    log.info("recreated %s.%s — workspace is clean", db, schema)


def _run_pipeline(database: str, schema: str, iterations: int, version: str):
    session = _session(with_database=True)

    # TODO: from forge.discovery import SchemaDiscovery
    # schema_profile = SchemaDiscovery(session).run(database, schema)
    schema_profile = None

    # TODO: from forge.writer import YAMLWriter
    # semantic_model = YAMLWriter(session).generate(schema_profile)
    semantic_model = None

    # TODO: from forge.scenarios import ScenarioGenerator
    # golden_set: [{"query": str, "expected_response": str}]
    # golden_set, questions = ScenarioGenerator(session).generate(schema_profile)
    golden_set: list[dict] = []
    questions: list[str] = []

    from forge.evaluator import (
        CortexAnalystApp,
        build_metrics,
        build_session,
        build_tru_app,
        get_results,
        run_evaluation,
    )

    tru_session = build_session(session)
    metrics = build_metrics(session, golden_set)

    for iteration in range(1, iterations + 1):
        log.info("iteration %d / %d", iteration, iterations)

        # TODO: from forge.probe import CortexAnalystProbe
        # probe = CortexAnalystProbe(session, semantic_model.to_yaml())
        probe = None

        iter_version = f"{version}.iter{iteration}"
        app = CortexAnalystApp(probe)
        tru_app = build_tru_app(app, version=iter_version)

        run_evaluation(tru_app, app, questions, metrics, version=iter_version)

        if not questions:
            log.info("no questions to score (stubs not yet implemented)")
        else:
            records_df, feedback_df = get_results(tru_session)
            mean_correctness = (
                feedback_df["answer_correctness"].mean() if not feedback_df.empty else 0.0
            )
            log.info(
                "scored %d questions — mean correctness: %.2f",
                len(records_df),
                mean_correctness,
            )

        # TODO: from forge.refiner import RefinementAgent
        # patch = RefinementAgent(session).refine(semantic_model, feedback_df)
        # if patch is None:
        #     log.info("no further refinements needed — stopping early")
        #     break
        # semantic_model = patch
        break  # remove once refiner is implemented

    log.info("done — view results in Snowsight → AI & ML → Evaluations")


def main():
    load_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")

    import argparse

    parser = argparse.ArgumentParser(
        prog="forge",
        description="Semantic Model Forge — Cortex Analyst semantic model generator and evaluator.",
    )
    parser.add_argument("--setup", action="store_true", help="Create workspace DB, schema, and network policy. Safe to re-run.")
    parser.add_argument("--reset-workspace", action="store_true", help="Drop and recreate the TruLens schema. Clears all evaluation records.")
    parser.add_argument("--yes", action="store_true", help="Skip confirmation prompt for --reset-workspace.")
    parser.add_argument("--database", help="Snowflake database to forge a model for (required for pipeline run).")
    parser.add_argument("--schema", help="Snowflake schema to forge a model for (required for pipeline run).")
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

    log.info("forging semantic model for %s.%s", args.database, args.schema)
    _run_pipeline(args.database, args.schema, args.iterations, args.version)


if __name__ == "__main__":
    main()
