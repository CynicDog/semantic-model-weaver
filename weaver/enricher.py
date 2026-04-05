"""
weaver.enricher — SynonymEnricher

Enriches a SemanticModel with human-readable descriptions and synonyms by
calling Snowflake Cortex (SNOWFLAKE.CORTEX.COMPLETE via session.sql).

No structural changes to the model — only description and synonyms fields
are filled in. If Cortex returns an unusable response for any table, that
table is left as-is and a warning is logged.

Usage:
    enriched = SynonymEnricher(session).enrich(model)
"""

from __future__ import annotations

import json
import logging
import re

from snowflake.snowpark import Session

from weaver.dsl import SemanticModel, SemanticTable

log = logging.getLogger(__name__)

_MODEL = "mistral-large2"

_SYSTEM_PROMPT = (
    "You are a data dictionary assistant. "
    "Given a database table and its columns, output ONLY a JSON object. "
    "No prose, no markdown fences, no explanation — just the JSON."
)


def _build_prompt(table: SemanticTable) -> str:
    lines = [f"Table: {table.name}"]
    for col in [*table.dimensions, *table.time_dimensions, *table.measures]:
        samples = ""
        if hasattr(col, "sample_values") and col.sample_values:
            samples = f"  sample values: {col.sample_values[:5]}"
        lines.append(f"  column: {col.name} ({col.data_type}){samples}")

    schema = {
        "table_description": "string",
        "columns": {
            "<column_name>": {
                "description": "string",
                "synonyms": ["string"],
            }
        },
    }
    return (
        "\n".join(lines)
        + "\n\nReturn JSON matching this shape:\n"
        + json.dumps(schema, indent=2)
    )


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


def _parse_response(raw: str) -> dict:
    match = re.search(r"\{.*\}", raw, re.DOTALL)
    if not match:
        raise ValueError("no JSON object found in Cortex response")
    return json.loads(match.group())


def _apply_enrichment(table: SemanticTable, data: dict) -> SemanticTable:
    col_data: dict = data.get("columns", {})
    table_desc: str = data.get("table_description", "")

    def _enrich(col):
        info = col_data.get(col.name, {})
        raw_synonyms = info.get("synonyms", col.synonyms) or col.synonyms
        synonyms = [str(s) for s in raw_synonyms if s is not None and str(s).strip()]
        updated = col.model_copy(update={
            "description": info.get("description", col.description) or col.description,
            "synonyms": synonyms,
        })
        return updated

    return table.model_copy(update={
        "description": table_desc or table.description,
        "dimensions": [_enrich(c) for c in table.dimensions],
        "time_dimensions": [_enrich(c) for c in table.time_dimensions],
        "measures": [_enrich(c) for c in table.measures],
    })


class SynonymEnricher:
    """
    Enriches a SemanticModel with descriptions and synonyms via Cortex.

    One Cortex call per table. Tables that fail enrichment are left unchanged.

    Usage:
        enriched = SynonymEnricher(session).enrich(model)
    """

    def __init__(self, session: Session) -> None:
        self._session = session

    def enrich(self, model: SemanticModel) -> SemanticModel:
        enriched_tables = []
        for table in model.tables:
            try:
                prompt = _build_prompt(table)
                raw = _call_cortex(self._session, prompt)
                data = _parse_response(raw)
                enriched_tables.append(_apply_enrichment(table, data))
                log.info("SynonymEnricher: enriched table %s", table.name)
            except Exception as exc:
                log.warning("SynonymEnricher: skipping %s — %s", table.name, exc)
                enriched_tables.append(table)

        return model.model_copy(update={"tables": enriched_tables})
