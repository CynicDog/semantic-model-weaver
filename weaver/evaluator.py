"""
weaver.evaluator — TruLens evaluation wrapper for CortexAnalystProbe.

Uses the OTEL / Snowflake event-table path:

  1. build_tru_app     — wraps CortexAnalystApp in TruApp (no feedbacks at construction)
  2. build_metrics     — returns server-side metric name strings (run entirely in Snowflake)
  3. run_evaluation    — fires questions via live_run, waits for ingestion, computes metrics
  4. get_results       — returns (records_df, feedback_df) for the leaderboard

All records and scores are logged to the Snowflake event table and visible in
Snowsight → AI & ML → Evaluations.

Metrics are server-side strings ("answer_relevance", "correctness") passed to
SYSTEM$EXECUTE_AI_OBSERVABILITY_RUN — no local Cortex Python SDK calls, no 403.
"""

from __future__ import annotations

import logging
import time
from typing import Any, Protocol, runtime_checkable

from trulens.apps.app import TruApp, instrument
from trulens.connectors.snowflake import SnowflakeConnector
from trulens.core import TruSession
from trulens.core.run import RunStatus

logger = logging.getLogger(__name__)

APP_NAME = "CortexAnalystProbe"
_POLL_INTERVAL = 3  # seconds between ingestion status checks


@runtime_checkable
class Probe(Protocol):
    """
    Interface expected by CortexAnalystApp.
    CortexAnalystProbe (weaver/probe.py) must satisfy this once implemented.

    Returns a dict with at minimum:
        answer  (str)       — answer text, or "" if unanswerable
        sql     (str|None)  — generated SQL, or None on failure
        success (bool)      — whether execution succeeded
    """

    def query(self, question: str) -> dict: ...


class CortexAnalystApp:
    """TruLens-instrumented wrapper around a Probe."""

    def __init__(self, probe: Probe) -> None:
        self.probe = probe
        self.results: dict[str, dict] = {}  # question → {answer, sql, success}

    @instrument
    def ask(self, question: str) -> str:
        """Fire question at Cortex Analyst; return the answer string."""
        result = self.probe.query(question)
        self.results[question] = result
        return result.get("answer", "")


def build_session(snowpark_session: Any) -> tuple[TruSession, SnowflakeConnector]:
    """Wire TruSession to Snowflake. Returns (tru_session, connector).

    The connector must be passed to build_tru_app so TruApp shares the same
    SnowflakeConnector instance — TruApp checks identity, not equality.
    """
    connector = SnowflakeConnector(snowpark_session=snowpark_session)
    return TruSession(connector=connector), connector


def build_metrics(snowpark_session: Any, golden_set: list[dict]) -> list[str]:
    """
    Return server-side metric name strings for the Snowflake OTEL evaluation path.

    Strings are passed directly to run.compute_metrics() which routes them to
    SYSTEM$EXECUTE_AI_OBSERVABILITY_RUN — computed entirely inside Snowflake
    without any local Cortex Python SDK calls (avoids the 403 from the REST API).

    Supported names: "answer_relevance", "correctness", "groundedness".
    golden_set and snowpark_session are kept as parameters for API compatibility
    and future use (e.g. uploading ground truth to a stage for "correctness").
    """
    return ["answer_relevance", "correctness"]


def build_tru_app(
    app: CortexAnalystApp,
    connector: SnowflakeConnector,
    version: str = "v1",
) -> TruApp:
    """
    Wrap CortexAnalystApp in TruApp.

    connector must be the same SnowflakeConnector instance used in build_session —
    TruApp checks identity (is not) when validating the connector against the
    active TruSession.
    """
    return TruApp(
        app=app,
        app_name=APP_NAME,
        app_version=version,
        main_method_name="ask",
        connector=connector,
        object_type="EXTERNAL AGENT",
    )


def run_evaluation(
    tru_app: TruApp,
    app: CortexAnalystApp,
    questions: list[str],
    metrics: list[str],
    version: str = "v1",
) -> None:
    """
    Fire questions through the instrumented app and score them.

    Flow:
      1. Open a live_run context — each question is one instrumented invocation.
      2. After the context exits, TruLens flushes OTEL spans and starts Snowflake ingestion.
      3. Poll until ingestion completes (INVOCATION_COMPLETED).
      4. Trigger metric computation via run.compute_metrics().
    """
    if not questions:
        return

    with tru_app.live_run(run_name=version) as live_run:
        for i, question in enumerate(questions):
            with live_run.input(str(i)):
                app.ask(question)
            logger.debug("invoked: %s", question)

    run = live_run.run

    ingestion_terminal = {
        RunStatus.INVOCATION_COMPLETED,
        RunStatus.INVOCATION_PARTIALLY_COMPLETED,
        RunStatus.FAILED,
    }
    status = run.get_status()
    while status not in ingestion_terminal:
        time.sleep(_POLL_INTERVAL)
        status = run.get_status()

    if status == RunStatus.FAILED:
        logger.warning("run %s failed during ingestion — skipping metric computation", version)
        return

    run.compute_metrics(metrics=metrics)
    logger.info("metrics computation triggered for run %s", version)

    metrics_terminal = {
        RunStatus.COMPLETED,
        RunStatus.PARTIALLY_COMPLETED,
        RunStatus.FAILED,
    }
    status = run.get_status()
    while status not in metrics_terminal:
        time.sleep(_POLL_INTERVAL)
        status = run.get_status()
        logger.debug("waiting for metrics computation: %s", status)

    if status == RunStatus.FAILED:
        logger.warning("run %s failed during metric computation", version)
    else:
        logger.info("metrics computation complete for run %s (status: %s)", version, status)


def get_results(
    tru_session: TruSession, snowpark_session: Any = None, version: str = ""
) -> tuple[Any, Any]:
    """Return (records_df, feedback_df) for the leaderboard.

    Primary path: direct SQL against GET_AI_OBSERVABILITY_EVENTS — returns
    scores + explanation per metric so the refiner knows *why* each question
    failed, not just that it did.

    Fallback: TruLens get_records_and_feedback (scores only, no explanations).
    """
    import pandas as pd

    # Primary path: direct SQL — includes explanation and criteria columns.
    if snowpark_session is not None:
        try:
            db = snowpark_session.get_current_database().strip('"')
            schema = snowpark_session.get_current_schema().strip('"')
            q = """
                WITH all_spans AS (
                    SELECT
                        RECORD_ATTRIBUTES:"ai.observability.span_type"::STRING AS span_type,
                        RECORD_ATTRIBUTES:"ai.observability.record_id"::STRING AS record_id,
                        RECORD_ATTRIBUTES:"ai.observability.record_root.input"::STRING AS input,
                        RECORD_ATTRIBUTES:"ai.observability.eval.metric_name"::STRING
                            AS metric_name,
                        RECORD_ATTRIBUTES:"ai.observability.eval.score"::FLOAT AS score,
                        RECORD_ATTRIBUTES:"ai.observability.eval.explanation"::STRING
                            AS explanation,
                        RECORD_ATTRIBUTES:"ai.observability.eval.criteria"::STRING AS criteria
                    FROM TABLE(SNOWFLAKE.LOCAL.GET_AI_OBSERVABILITY_EVENTS(?, ?, ?, ?))
                    WHERE RECORD_ATTRIBUTES:"ai.observability.run.name"::STRING = ?
                       OR RECORD_ATTRIBUTES:"snow.ai.observability.run.name"::STRING = ?
                ),
                records AS (
                    SELECT record_id, MAX(input) AS input
                    FROM all_spans
                    WHERE span_type = 'record_root' AND input IS NOT NULL
                    GROUP BY record_id
                ),
                evals AS (
                    SELECT record_id, metric_name,
                           AVG(score) AS score,
                           MAX(explanation) AS explanation,
                           MAX(criteria) AS criteria
                    FROM all_spans
                    WHERE span_type = 'eval'
                      AND metric_name IS NOT NULL AND score IS NOT NULL
                    GROUP BY record_id, metric_name
                )
                SELECT r.input, e.metric_name, e.score, e.explanation, e.criteria
                FROM records r
                JOIN evals e ON e.record_id = r.record_id
            """
            rows = snowpark_session.sql(
                q, params=[db, schema, APP_NAME, "EXTERNAL AGENT", version, version]
            ).to_pandas()
            rows.columns = rows.columns.str.lower()
            if not rows.empty:
                score_pivot = rows.pivot_table(
                    index="input", columns="metric_name", values="score", aggfunc="mean"
                ).reset_index()
                score_pivot.columns.name = None

                expl_pivot = rows.pivot_table(
                    index="input",
                    columns="metric_name",
                    values="explanation",
                    aggfunc=lambda x: next((v for v in x if pd.notna(v) and v), ""),
                ).reset_index()
                expl_pivot.columns.name = None
                expl_pivot.rename(
                    columns={c: f"{c}_explanation" for c in expl_pivot.columns if c != "input"},
                    inplace=True,
                )

                feedback_df = score_pivot.merge(expl_pivot, on="input", how="left")
                if (
                    "answer_relevance" in feedback_df.columns
                    and "correctness" not in feedback_df.columns
                ):
                    feedback_df["correctness"] = feedback_df["answer_relevance"]

                logger.info("get_results: %d questions scored via direct SQL", len(feedback_df))
                return feedback_df[["input"]].copy(), feedback_df

            logger.warning("get_results: no scored rows found for version '%s'", version)
        except Exception as exc:
            logger.warning("get_results direct SQL failed (%s) — falling back to TruLens", exc)

    # Fallback: TruLens get_records_and_feedback (no explanation columns).
    try:
        records_df, feedback_cols = tru_session.get_records_and_feedback(
            app_name=APP_NAME, run_name=version or None
        )
        if not records_df.empty and feedback_cols:
            feedback_df = records_df[["input"] + list(feedback_cols)].copy()
            if (
                "answer_relevance" in feedback_df.columns
                and "correctness" not in feedback_df.columns
            ):
                feedback_df["correctness"] = feedback_df["answer_relevance"]
            logger.info("get_results: %d questions scored via TruLens fallback", len(feedback_df))
            return records_df, feedback_df
    except Exception as exc:
        logger.warning("get_results TruLens fallback also failed (%s)", exc)

    return pd.DataFrame(), pd.DataFrame()
