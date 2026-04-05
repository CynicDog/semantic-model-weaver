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

    @instrument
    def ask(self, question: str) -> str:
        """Fire question at Cortex Analyst; return the answer string."""
        result = self.probe.query(question)
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


def get_results(tru_session: TruSession, snowpark_session: Any = None, version: str = "") -> tuple[Any, Any]:
    """Return (records_df, feedback_df) for the leaderboard.

    Tries TruLens get_records_and_feedback first; falls back to querying
    GET_AI_OBSERVABILITY_EVENTS_NORMALIZED directly when TruLens fails to
    deserialize records (known issue with null RECORD fields in OTEL path).
    """
    import pandas as pd

    try:
        return tru_session.get_records_and_feedback(app_name=APP_NAME)
    except Exception as exc:
        logger.warning("get_results via TruLens failed (%s) — falling back to direct SQL", exc)

    if snowpark_session is None:
        return pd.DataFrame(), pd.DataFrame()

    try:
        db = snowpark_session.get_current_database().strip('"')
        schema = snowpark_session.get_current_schema().strip('"')
        q = f"""
            SELECT INPUT, METRIC_NAME, EVAL_SCORE
            FROM TABLE(SNOWFLAKE.LOCAL.GET_AI_OBSERVABILITY_EVENTS_NORMALIZED(
                '{db}', '{schema}', '{APP_NAME}', 'EXTERNAL AGENT'
            ))
            WHERE METRIC_NAME IS NOT NULL
              AND EVAL_SCORE IS NOT NULL
        """
        if version:
            q += f" AND RUN_NAME = '{version}'"
        rows = snowpark_session.sql(q).to_pandas()
        rows.columns = rows.columns.str.lower()
        if rows.empty:
            return pd.DataFrame(), pd.DataFrame()

        feedback_df = rows.pivot_table(
            index="input", columns="metric_name", values="eval_score", aggfunc="mean"
        ).reset_index()
        feedback_df.columns.name = None
        logger.info("get_results: fetched %d scored rows via direct SQL", len(rows))
        return pd.DataFrame({"input": feedback_df["input"]}), feedback_df
    except Exception as exc:
        logger.warning("get_results direct SQL also failed (%s) — returning empty frames", exc)
        return pd.DataFrame(), pd.DataFrame()
