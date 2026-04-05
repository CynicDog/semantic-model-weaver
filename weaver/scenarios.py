"""
weaver.scenarios — ScenarioGenerator

Generates natural-language questions and ground-truth expected responses for
evaluating a Cortex Analyst semantic model.

For each table (and related table pairs), one Cortex call produces a batch of
scenarios as JSON: {"scenarios": [{"question": str, "sql": str}, ...]}.
Each SQL is executed directly against Snowflake to produce an expected_response
string. Scenarios where the SQL fails are silently dropped.

Return shape:
    golden_set  — [{"query": str, "expected_response": str}]  (for TruLens)
    questions   — [str]  (same questions, plain list for probe calls)

Usage:
    golden_set, questions = ScenarioGenerator(session).generate(schema_profile)
"""

from __future__ import annotations

import json
import logging
import re

from snowflake.snowpark import Session

from weaver.discovery import SchemaProfile

log = logging.getLogger(__name__)

_MODEL = "mistral-large2"
_SCENARIOS_PER_TABLE = 5
_MAX_RESULT_ROWS = 3

_SYSTEM_PROMPT = (
    "You are a data analyst writing test questions for a BI semantic layer. "
    "Given a database table schema, output ONLY a JSON object — no prose, no markdown fences."
)


def _col_summary(col: dict) -> str:
    parts = [f"{col['name']} ({col['type']})"]
    if col.get("sample_values"):
        parts.append(f"sample: {col['sample_values'][:3]}")
    if col.get("comment"):
        parts.append(f"// {col['comment']}")
    return "  " + " | ".join(parts)


def _build_prompt(table: dict, database: str, schema: str, related: list[dict]) -> str:
    lines = [
        f"Table: {database}.{schema}.{table['name']}",
        "Columns:",
    ]
    for col in table["columns"]:
        lines.append(_col_summary(col))

    if related:
        lines.append("Related tables (joinable via shared column names):")
        for rt in related:
            col_names = [c["name"] for c in rt["columns"]]
            lines.append(f"  {database}.{schema}.{rt['name']}: {', '.join(col_names[:10])}")

    schema_hint = {
        "scenarios": [
            {
                "question": "plain English question",
                "sql": f"SELECT ... FROM {database}.{schema}.{table['name']} ...",
            }
        ]
    }

    lines += [
        "",
        f"Write {_SCENARIOS_PER_TABLE} test questions an analyst might ask about this data.",
        "Rules:",
        "  - Each question must be answerable with a single SQL query.",
        "  - Include a mix of aggregations, filters, time-based, and (if related tables exist) joins.",
        "  - SQL must reference fully-qualified table names.",
        "  - SQL must be valid Snowflake SQL.",
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
        f" {{'temperature': 0.3}}"
        f")"
    )
    row = session.sql(sql).collect()
    raw = row[0][0] if row else ""
    return _unwrap_cortex(raw)


def _unwrap_cortex(raw: str) -> str:
    """CORTEX.COMPLETE returns {"choices": [{"messages": "<text>"}]} — extract the text."""
    try:
        return json.loads(raw)["choices"][0]["messages"]
    except Exception:
        return raw


def _parse_scenarios(raw: str) -> list[dict]:
    match = re.search(r"\{.*\}", raw, re.DOTALL)
    if not match:
        raise ValueError("no JSON object found in Cortex response")
    data = json.loads(match.group())
    scenarios = data.get("scenarios", [])
    return [s for s in scenarios if s.get("question") and s.get("sql")]


def _execute_sql(session: Session, sql: str) -> str | None:
    try:
        rows = session.sql(sql).collect()
        if not rows:
            return "No results."
        sample = rows[:_MAX_RESULT_ROWS]
        result = [row.as_dict() for row in sample]
        suffix = (
            f" (showing {len(sample)} of {len(rows)} rows)"
            if len(rows) > _MAX_RESULT_ROWS
            else ""
        )
        return json.dumps(result, default=str) + suffix
    except Exception as exc:
        log.warning("ScenarioGenerator: SQL failed — %s | sql: %s", exc, sql[:200])
        return None


def _find_related(table: dict, all_tables: list[dict]) -> list[dict]:
    col_names = {c["name"] for c in table["columns"]}
    return [
        other for other in all_tables
        if other["name"] != table["name"]
        and col_names & {c["name"] for c in other["columns"]}
    ]


class ScenarioGenerator:
    """
    Generates NL questions and ground-truth expected responses for a schema.

    One Cortex call per table, SQL executed directly against Snowflake.
    Tables where generation or SQL execution fails are skipped gracefully.

    Usage:
        golden_set, questions = ScenarioGenerator(session).generate(profile)
    """

    def __init__(self, session: Session) -> None:
        self._session = session

    def generate(self, profile: SchemaProfile) -> tuple[list[dict], list[str]]:
        database = profile["database"]
        schema = profile["schema"]
        all_tables = profile["tables"]

        golden_set: list[dict] = []
        questions: list[str] = []

        for table in all_tables:
            related = _find_related(table, all_tables)
            try:
                prompt = _build_prompt(table, database, schema, related)
                raw = _call_cortex(self._session, prompt)
                scenarios = _parse_scenarios(raw)
            except Exception as exc:
                log.warning(
                    "ScenarioGenerator: skipping %s (generation failed) — %s",
                    table["name"], exc,
                )
                continue

            kept = 0
            for scenario in scenarios:
                question = scenario["question"]
                sql = scenario["sql"]
                expected = _execute_sql(self._session, sql)
                if expected is None:
                    log.debug("ScenarioGenerator: dropped (SQL failed): %s", question)
                    continue
                golden_set.append({"query": question, "expected_response": expected})
                questions.append(question)
                kept += 1

            log.info(
                "ScenarioGenerator: %s → %d/%d scenarios kept",
                table["name"], kept, len(scenarios),
            )

        log.info(
            "ScenarioGenerator: total %d scenarios across %d tables",
            len(questions), len(all_tables),
        )
        return golden_set, questions
