"""
Entry point: uv run python -m forge --database <DB> --schema <SCHEMA>

Pipeline stages (implemented stages are wired; stubs are marked TODO):

  [SchemaDiscovery]   → schema_profile           (TODO: forge/discovery.py)
  [YAMLWriter]        → semantic_model (YAML)     (TODO: forge/writer.py)
  [ScenarioGenerator] → golden_set, questions     (TODO: forge/scenarios.py)
  [EvalLogger+TruLens]→ records_df, feedback_df   ← implemented (forge/evaluator.py)
  [RefinementAgent]   → refined semantic_model    (TODO: forge/refiner.py)
"""

import argparse
import logging
import os

from dotenv import load_dotenv

log = logging.getLogger(__name__)


def _snowpark_session():
    from snowflake.snowpark import Session

    return Session.builder.configs({
        "account": os.environ["FORGE_SNOWFLAKE_ACCOUNT"],
        "user": os.environ["FORGE_SNOWFLAKE_USER"],
        "password": os.environ["FORGE_SNOWFLAKE_PASSWORD"],
        "role": os.environ.get("FORGE_SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
        "warehouse": os.environ.get("FORGE_SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
    }).create()


def main():
    load_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")

    parser = argparse.ArgumentParser(
        prog="forge",
        description="Generate and evaluate a Cortex Analyst semantic model from a Snowflake schema.",
    )
    parser.add_argument("--database", required=True, help="Snowflake database name")
    parser.add_argument("--schema", required=True, help="Snowflake schema name")
    parser.add_argument(
        "--iterations", type=int, default=3, help="Max refinement iterations (default: 3)"
    )
    parser.add_argument(
        "--version", default="v1", help="TruLens app version tag (default: v1)"
    )
    args = parser.parse_args()

    log.info("Forging semantic model for %s.%s ...", args.database, args.schema)

    session = _snowpark_session()

    # ── Stage 1: SchemaDiscovery ─────────────────────────────────────────────
    # TODO: from forge.discovery import SchemaDiscovery
    # schema_profile = SchemaDiscovery(session).run(args.database, args.schema)
    schema_profile = None  # stub

    # ── Stage 2: YAMLWriter ──────────────────────────────────────────────────
    # TODO: from forge.writer import YAMLWriter
    # semantic_model = YAMLWriter(session).generate(schema_profile)
    semantic_model = None  # stub

    # ── Stage 3: ScenarioGenerator ───────────────────────────────────────────
    # Produces:
    #   golden_set — [{"query": str, "expected_response": str}]
    #                answers derived from direct SQL queries, not from any YAML
    #   questions  — the NL question strings to fire at Cortex Analyst
    # TODO: from forge.scenarios import ScenarioGenerator
    # golden_set, questions = ScenarioGenerator(session).generate(schema_profile)
    golden_set: list[dict] = []   # stub
    questions: list[str] = []    # stub

    # ── Stage 4: Evaluation (TruLens) ────────────────────────────────────────
    from forge.evaluator import (
        CortexAnalystApp,
        build_feedbacks,
        build_session,
        build_tru_app,
        get_results,
        run_evaluation,
    )

    tru_session = build_session(session)
    feedbacks = build_feedbacks(session, golden_set)

    for iteration in range(1, args.iterations + 1):
        log.info("── Iteration %d / %d ──", iteration, args.iterations)

        # TODO: replace stub probe with real CortexAnalystProbe once probe.py is implemented
        # from forge.probe import CortexAnalystProbe
        # probe = CortexAnalystProbe(session, semantic_model.to_yaml())
        probe = None  # stub

        app = CortexAnalystApp(probe)
        tru_app = build_tru_app(app, feedbacks, version=f"{args.version}.iter{iteration}")

        run_evaluation(tru_app, app, questions)

        records_df, feedback_df = get_results(tru_session)
        mean_correctness = (
            feedback_df["answer_correctness"].mean() if not feedback_df.empty else 0.0
        )
        log.info(
            "Scored %d questions — mean correctness: %.2f",
            len(records_df),
            mean_correctness,
        )

        # ── Stage 5: RefinementAgent ─────────────────────────────────────────
        # Reads failure report from feedback_df and patches the semantic YAML.
        # TODO: from forge.refiner import RefinementAgent
        # patch = RefinementAgent(session).refine(semantic_model, feedback_df)
        # if patch is None:
        #     log.info("No further refinements needed — stopping early.")
        #     break
        # semantic_model = patch
        break  # remove once refiner is implemented

    log.info("Done. View results in Snowsight → AI & ML → Evaluations.")


if __name__ == "__main__":
    main()
