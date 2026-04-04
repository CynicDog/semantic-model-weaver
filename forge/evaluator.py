"""
forge.evaluator — TruLens evaluation wrapper for CortexAnalystProbe.

Each NL question fired at Cortex Analyst becomes a TruLens *record*. Feedback
functions score the answer against the ground truth derived by ScenarioGenerator
(direct SQL queries — never a hand-crafted YAML). All records and scores are
logged to Snowflake and visible in Snowsight → AI & ML → Evaluations.

Public surface
--------------
  CortexAnalystApp   — @instrument()-decorated wrapper; satisfies the Probe Protocol
  build_session      — wires TruSession → SnowflakeConnector
  build_feedbacks    — answer_relevance + answer_correctness (Cortex provider)
  build_tru_app      — wraps CortexAnalystApp in TruApp
  run_evaluation     — fires each question through the instrumented context
  get_results        — returns (records_df, feedback_df) for the leaderboard
"""

from __future__ import annotations

import logging
from typing import Any, Protocol, runtime_checkable

from trulens.apps.app import TruApp, instrument
from trulens.connectors.snowflake import SnowflakeConnector
from trulens.core import Feedback, TruSession
from trulens.feedback import GroundTruthAgreement
from trulens.providers.cortex import Cortex

logger = logging.getLogger(__name__)

APP_NAME = "CortexAnalystProbe"


@runtime_checkable
class Probe(Protocol):
    """
    Interface expected by CortexAnalystApp.
    CortexAnalystProbe (forge/probe.py) must satisfy this once implemented.
    """

    def query(self, question: str) -> dict:
        """
        Fire a natural-language question at Cortex Analyst.

        Returns a dict with at minimum:
            answer  (str)  — the answer text, or "" if unanswerable
            sql     (str | None) — generated SQL, or None on failure
            success (bool) — whether execution succeeded
        """
        ...


class CortexAnalystApp:
    """
    TruLens-instrumented wrapper around a Probe.

    The @instrument() decorator marks ask() so TruApp records inputs/outputs.
    Calling ask() on a bare CortexAnalystApp (not wrapped in TruApp) is a no-op
    instrumentation-wise — it simply delegates to probe.query().
    """

    def __init__(self, probe: Probe) -> None:
        self.probe = probe

    @instrument
    def ask(self, question: str) -> str:
        """Fire question at Cortex Analyst; return the answer string."""
        result = self.probe.query(question)
        return result.get("answer", "")


def build_session(snowpark_session: Any) -> TruSession:
    """
    Create a TruSession wired to Snowflake.
    All subsequent TruApp recordings auto-log to TRULENS_RECORDS /
    TRULENS_FEEDBACK_RESULTS in the connected Snowflake schema.
    """
    connector = SnowflakeConnector(snowpark_session=snowpark_session)
    return TruSession(connector=connector)


def build_feedbacks(snowpark_session: Any, golden_set: list[dict]) -> list[Feedback]:
    """
    Build feedback functions that score each Cortex Analyst answer.

    Args:
        snowpark_session: active Snowpark session (Cortex provider uses it
                          to call COMPLETE() inside Snowflake — no external API).
        golden_set: list of {"query": str, "expected_response": str} dicts.
                    Produced by ScenarioGenerator from direct SQL queries.

    Returns:
        [f_answer_relevance, f_answer_correctness]
    """
    provider = Cortex(snowpark_session=snowpark_session, model_engine="llama3.1-8b")

    f_relevance = (
        Feedback(provider.relevance_with_cot_reasons, name="answer_relevance")
        .on_input_output()
    )

    gt = GroundTruthAgreement(golden_set, provider=provider)
    f_correctness = (
        Feedback(gt.agreement_measure, name="answer_correctness")
        .on_input_output()
    )

    return [f_relevance, f_correctness]


def build_tru_app(
    app: CortexAnalystApp,
    feedbacks: list[Feedback],
    version: str = "v1",
) -> TruApp:
    """Wrap CortexAnalystApp in TruApp for instrumented recording."""
    return TruApp(
        app=app,
        app_name=APP_NAME,
        app_version=version,
        feedbacks=feedbacks,
    )


def run_evaluation(
    tru_app: TruApp,
    app: CortexAnalystApp,
    questions: list[str],
) -> None:
    """
    Fire each question through the instrumented app.

    Each `with tru_app` block is one TruLens record. The loop is intentionally
    serial: Cortex Analyst and TruLens scoring are both network I/O; parallelism
    would complicate record attribution without meaningful throughput gain.
    """
    for question in questions:
        with tru_app:
            app.ask(question)
        logger.debug("recorded: %s", question)


def get_results(tru_session: TruSession) -> tuple[Any, Any]:
    """
    Return (records_df, feedback_df) for the leaderboard.

    records_df   — one row per probe call (input, output, timestamps)
    feedback_df  — scores per record (answer_relevance, answer_correctness)
    """
    return tru_session.get_records_and_feedback(app_name=APP_NAME)
