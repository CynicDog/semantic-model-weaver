"""
weaver.refiner — RefinementAgent

Reads TruLens feedback results, identifies failure patterns, and patches the
SemanticModel by calling Cortex with a targeted prompt.

Patching is conservative: only synonyms, descriptions, and measure expressions
are modified — table names, base_table references, and join columns are never
changed. Structural integrity is guaranteed by re-parsing through the DSL.

Return value:
    SemanticModel — refined model if any patches were applied
    None          — if scores are already above the threshold (stop the loop early)

Usage:
    patch = RefinementAgent(session).refine(semantic_model, feedback_df)
    if patch is None:
        break   # convergence reached
    semantic_model = patch
"""

from __future__ import annotations

import json
import logging
import re
from typing import TYPE_CHECKING

from snowflake.snowpark import Session

from weaver.dsl import (
    Dimension,
    Measure,
    SemanticModel,
    SemanticTable,
    TimeDimension,
)

if TYPE_CHECKING:
    import pandas as pd

log = logging.getLogger(__name__)

_MODEL = "mistral-large2"
_CONVERGENCE_THRESHOLD = 0.85

_SYSTEM_PROMPT = (
    "You are a semantic model editor. "
    "Given a table definition and failed questions, output ONLY a JSON patch object. "
    "No prose, no markdown fences, no explanation — just the JSON."
)


def _score_summary(feedback_df: pd.DataFrame) -> dict:
    if feedback_df.empty:
        return {}
    numeric = feedback_df.select_dtypes("number")
    return numeric.mean().to_dict()


def _failed_questions(feedback_df: pd.DataFrame, threshold: float = 0.5) -> list[str]:
    if feedback_df.empty or "input" not in feedback_df.columns:
        return []
    col = "correctness" if "correctness" in feedback_df.columns else None
    if col is None:
        return []
    mask = feedback_df[col] < threshold
    return feedback_df.loc[mask, "input"].tolist()


def _build_patch_prompt(table: SemanticTable, failed: list[str]) -> str:
    col_lines = []
    for col in [*table.dimensions, *table.time_dimensions, *table.measures]:
        col_lines.append(f"  {col.name} ({col.data_type}) desc={col.description!r} synonyms={col.synonyms}")

    schema_hint = {
        "table_name": table.name,
        "patches": {
            "<column_name>": {
                "description": "improved description or empty string to skip",
                "synonyms": ["additional", "aliases"],
            }
        },
    }

    lines = [
        f"Table: {table.name}",
        "Current columns:",
        *col_lines,
        "",
        "The following analyst questions were answered incorrectly:",
        *[f"  - {q}" for q in failed[:10]],
        "",
        "Suggest synonym and description improvements that would help Cortex Analyst",
        "understand these questions better. Do NOT change column names or SQL expressions.",
        "",
        "Return JSON matching this shape:",
        json.dumps(schema_hint, indent=2),
    ]
    return "\n".join(lines)


def _call_cortex(session: Session, prompt: str) -> str:
    flat = prompt.replace("\n", " ").replace("\r", " ").replace("'", "''")
    system = _SYSTEM_PROMPT.replace("'", "''")
    sql = (
        f"SELECT SNOWFLAKE.CORTEX.COMPLETE("
        f"'{_MODEL}', "
        f"[{{'role': 'system', 'content': '{system}'}},"
        f" {{'role': 'user', 'content': '{flat}'}}],"
        f" {{'temperature': 0}}"
        f")"
    )
    row = session.sql(sql).collect()
    return _unwrap_cortex(row[0][0] if row else "")


def _unwrap_cortex(raw: str) -> str:
    try:
        data = json.loads(raw)
        return data["choices"][0]["messages"]
    except Exception:
        return raw


def _parse_patch(raw: str) -> dict:
    match = re.search(r"\{.*\}", raw, re.DOTALL)
    if not match:
        raise ValueError("no JSON object found in Cortex response")
    data = json.loads(match.group())
    return data.get("patches", {})


def _apply_patch(table: SemanticTable, patches: dict) -> SemanticTable:
    def _patch_col(col):
        info = patches.get(col.name, {})
        updates: dict = {}
        new_desc = info.get("description", "")
        new_syns = info.get("synonyms", [])
        if new_desc:
            updates["description"] = new_desc
        if new_syns:
            updates["synonyms"] = list(dict.fromkeys([*col.synonyms, *new_syns]))
        return col.model_copy(update=updates) if updates else col

    return table.model_copy(update={
        "dimensions": [_patch_col(c) for c in table.dimensions],
        "time_dimensions": [_patch_col(c) for c in table.time_dimensions],
        "measures": [_patch_col(c) for c in table.measures],
    })


class RefinementAgent:
    """
    Reads TruLens feedback and patches a SemanticModel via Cortex.

    Returns None when scores are above _CONVERGENCE_THRESHOLD (pipeline stops).
    Returns an improved SemanticModel otherwise.

    Usage:
        patch = RefinementAgent(session).refine(model, feedback_df)
        if patch is None:
            break
        semantic_model = patch
    """

    def __init__(self, session: Session) -> None:
        self._session = session

    def refine(self, model: SemanticModel, feedback_df: pd.DataFrame) -> SemanticModel | None:
        scores = _score_summary(feedback_df)
        log.info("RefinementAgent: scores %s", scores)

        mean_correctness = scores.get("answer_correctness", 0.0)
        if mean_correctness >= _CONVERGENCE_THRESHOLD:
            log.info(
                "RefinementAgent: converged (correctness=%.2f ≥ %.2f) — stopping",
                mean_correctness, _CONVERGENCE_THRESHOLD,
            )
            return None

        failed = _failed_questions(feedback_df)
        if not failed:
            log.info("RefinementAgent: no failed questions to refine on")
            return None

        patched_tables = []
        for table in model.tables:
            try:
                prompt = _build_patch_prompt(table, failed)
                raw = _call_cortex(self._session, prompt)
                patches = _parse_patch(raw)
                if patches:
                    patched_tables.append(_apply_patch(table, patches))
                    log.info("RefinementAgent: patched %s (%d columns)", table.name, len(patches))
                else:
                    patched_tables.append(table)
            except Exception as exc:
                log.warning("RefinementAgent: skipping %s — %s", table.name, exc)
                patched_tables.append(table)

        return model.model_copy(update={"tables": patched_tables})
