"""
forge.evaluator — TruLens evaluation wrapper for CortexAnalystProbe.

Uses the OTEL / Snowflake event-table path:

  1. build_tru_app     — wraps CortexAnalystApp in TruApp (no feedbacks at construction)
  2. build_metrics     — creates Metric objects for answer_relevance + answer_correctness
  3. run_evaluation    — fires questions via live_run, waits for ingestion, computes metrics
  4. get_results       — returns (records_df, feedback_df) for the leaderboard

All records and scores are logged to the Snowflake event table and visible in
Snowsight → AI & ML → Evaluations.
"""

from __future__ import annotations

import logging
import time
from typing import Any, Protocol, runtime_checkable

from trulens.apps.app import TruApp, instrument
from trulens.connectors.snowflake import SnowflakeConnector
from trulens.core import Metric, TruSession
from trulens.core.run import RunStatus
from trulens.feedback import GroundTruthAgreement
from trulens.providers.cortex import Cortex

logger = logging.getLogger(__name__)

APP_NAME = "CortexAnalystProbe"
_POLL_INTERVAL = 3  # seconds between ingestion status checks


@runtime_checkable
class Probe(Protocol):
    """
    Interface expected by CortexAnalystApp.
    CortexAnalystProbe (forge/probe.py) must satisfy this once implemented.

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


def build_session(snowpark_session: Any) -> TruSession:
    """Wire TruSession to Snowflake. All recordings auto-log to the event table."""
    connector = SnowflakeConnector(snowpark_session=snowpark_session)
    return TruSession(connector=connector)


def build_metrics(snowpark_session: Any, golden_set: list[dict]) -> list[Metric]:
    """
    Build Metric objects for the OTEL evaluation path.

    Metrics are NOT attached to TruApp at construction — they are passed
    explicitly to run.compute_metrics() after invocations complete.

    Args:
        snowpark_session: active Snowpark session (Cortex provider uses COMPLETE()
                          inside Snowflake — no external API key needed).
        golden_set: [{"query": str, "expected_response": str}]
                    produced by ScenarioGenerator from direct SQL queries.

    Returns:
        [m_answer_relevance, m_answer_correctness]
    """
    provider = Cortex(snowpark_session=snowpark_session, model_engine="llama3.1-8b")

    m_relevance = (
        Metric(implementation=provider.relevance_with_cot_reasons, name="answer_relevance")
        .on_input_output()
    )

    gt = GroundTruthAgreement(golden_set, provider=provider)
    m_correctness = (
        Metric(implementation=gt.agreement_measure, name="answer_correctness")
        .on_input_output()
    )

    return [m_relevance, m_correctness]


def build_tru_app(app: CortexAnalystApp, version: str = "v1") -> TruApp:
    """
    Wrap CortexAnalystApp in TruApp.

    No feedbacks at construction — this is the OTEL path where metrics are
    computed explicitly via run.compute_metrics() after the run completes.
    main_method_name tells TruApp which method is the app's entry point.
    """
    return TruApp(
        app=app,
        app_name=APP_NAME,
        app_version=version,
        main_method_name="ask",
    )


def run_evaluation(
    tru_app: TruApp,
    app: CortexAnalystApp,
    questions: list[str],
    metrics: list[Metric],
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

    terminal = {
        RunStatus.INVOCATION_COMPLETED,
        RunStatus.INVOCATION_PARTIALLY_COMPLETED,
        RunStatus.FAILED,
    }
    status = run.get_status()
    while status not in terminal:
        time.sleep(_POLL_INTERVAL)
        status = run.get_status()

    if status == RunStatus.FAILED:
        logger.warning("run %s failed during ingestion — skipping metric computation", version)
        return

    run.compute_metrics(metrics=metrics)
    logger.info("metrics computation triggered for run %s", version)


def get_results(tru_session: TruSession) -> tuple[Any, Any]:
    """Return (records_df, feedback_df) for the leaderboard."""
    return tru_session.get_records_and_feedback(app_name=APP_NAME)
