"""
weaver.enricher — SynonymEnricher

Enriches a SemanticModel with human-readable descriptions and synonyms by
calling Snowflake Cortex (SNOWFLAKE.CORTEX.COMPLETE via session.sql).

No structural changes to the model — only description and synonyms fields
are filled in. If Cortex returns an unusable response for any table, that
table is left as-is and a warning is logged.

Usage:
    query_terms = QueryHistoryMiner(session).mine(database, schema)
    enriched = SynonymEnricher(session).enrich(model, query_terms=query_terms)
"""

from __future__ import annotations

import json
import logging
import re

from snowflake.snowpark import Session

from weaver.dsl import SemanticModel, SemanticTable
from weaver.query_history import QueryTerms

log = logging.getLogger(__name__)

_MODEL = "mistral-large2"

_KNOWN_DATA_TYPES = {
    "VARCHAR", "NUMBER", "FLOAT", "DATE", "BOOLEAN",
    "TIMESTAMP", "TIMESTAMP_NTZ", "TIMESTAMP_LTZ", "TIMESTAMP_TZ",
    "VARIANT", "OBJECT", "ARRAY", "INT", "INTEGER", "TEXT", "STRING",
    "CHAR", "BIGINT", "SMALLINT", "DECIMAL", "NUMERIC", "DOUBLE",
}

_SYSTEM_PROMPT = (
    "You are a data dictionary assistant for a Korean analytics platform. "
    "Given a database table and its columns, output ONLY a JSON object. "
    "No prose, no markdown fences, no explanation — just the JSON."
)


def _build_prompt(table: SemanticTable, query_terms: QueryTerms) -> str:
    table_terms = query_terms.get(table.name, {})

    lines = [
        f"Table: {table.name}",
        "",
        "For each column below, provide:",
        "  - description: a concise business definition (1 sentence, in English)",
        "  - synonyms: 3-6 natural-language phrases an analyst would type to refer to this column",
        "              Include BOTH English phrases AND Korean (한국어) equivalents.",
        "              Example for a trade volume column: "
        "['trade volume', 'trading volume', '거래량', '거래대금']",
        "",
        "RULES:",
        "  - Synonyms MUST be human-readable business phrases",
        "  - Always include at least 1-2 Korean synonyms per column when the meaning is clear",
        "  - Do NOT use raw data values as synonyms (e.g. 'KR7225570001', 'A225570')",
        "  - Do NOT use SQL identifiers or column names as synonyms",
        "  - Do NOT use data type names as descriptions",
        "  - If you cannot confidently infer synonyms, return an empty list for that column",
        "",
        "Columns:",
    ]

    for col in [*table.dimensions, *table.time_dimensions, *table.measures]:
        parts = [f"  {col.name} ({col.data_type})"]
        if col.description and col.description.upper().strip() not in _KNOWN_DATA_TYPES:
            parts.append(f"comment: {col.description!r}")
        mined = table_terms.get(col.name, [])
        if mined:
            parts.append(f"query-history aliases: {mined}")
        lines.append("  ".join(parts) if len(parts) > 1 else parts[0])

    schema = {
        "table_description": "string",
        "columns": {
            "<column_name>": {
                "description": "string",
                "synonyms": ["string"],
            }
        },
    }
    lines += ["", "Return JSON matching this shape:", json.dumps(schema, indent=2)]
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


def _parse_response(raw: str) -> dict:
    match = re.search(r"\{.*\}", raw, re.DOTALL)
    if not match:
        raise ValueError("no JSON object found in Cortex response")
    return json.loads(match.group())


def _clean_description(desc: str) -> str:
    """Reject descriptions that are just a data type token echoed back by the LLM."""
    if not desc:
        return ""
    if desc.upper().strip() in _KNOWN_DATA_TYPES:
        return ""
    return desc


def _apply_enrichment(table: SemanticTable, data: dict) -> SemanticTable:
    col_data: dict = data.get("columns", {})
    table_desc: str = _clean_description(data.get("table_description", ""))

    def _enrich(col):
        info = col_data.get(col.name, {})

        raw_desc = _clean_description(info.get("description", ""))
        new_desc = raw_desc or col.description

        raw_synonyms = info.get("synonyms", []) or []
        synonyms = [
            str(s).strip() for s in raw_synonyms
            if s is not None
            and str(s).strip()
            and str(s).upper().strip() not in _KNOWN_DATA_TYPES
            and not _looks_like_raw_value(str(s))
        ]

        return col.model_copy(update={
            "description": new_desc,
            "synonyms": synonyms if synonyms else col.synonyms,
        })

    return table.model_copy(update={
        "description": table_desc or table.description,
        "dimensions": [_enrich(c) for c in table.dimensions],
        "time_dimensions": [_enrich(c) for c in table.time_dimensions],
        "measures": [_enrich(c) for c in table.measures],
    })


def _looks_like_raw_value(s: str) -> bool:
    """
    Heuristic: reject strings that look like raw data values rather than
    business phrases — e.g. ISIN codes, ticker codes, numeric strings.
    """
    s = s.strip()
    if re.fullmatch(r"[A-Z]{2}[A-Z0-9]{10}", s):
        return True
    if re.fullmatch(r"[A-Z]\d{6}", s):
        return True
    if re.fullmatch(r"\d+", s):
        return True
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
        return True
    return False


class SynonymEnricher:
    """
    Enriches a SemanticModel with descriptions and synonyms via Cortex.

    Accepts optional query_terms from QueryHistoryMiner to ground synonyms in
    real business vocabulary extracted from historical SQL.

    One Cortex call per table. Tables that fail enrichment are left unchanged.

    Usage:
        enriched = SynonymEnricher(session).enrich(model, query_terms=terms)
    """

    def __init__(self, session: Session) -> None:
        self._session = session

    def enrich(
        self,
        model: SemanticModel,
        query_terms: QueryTerms | None = None,
    ) -> SemanticModel:
        terms = query_terms or {}
        enriched_tables = []
        for table in model.tables:
            try:
                prompt = _build_prompt(table, terms)
                raw = _call_cortex(self._session, prompt)
                data = _parse_response(raw)
                enriched_tables.append(_apply_enrichment(table, data))
                log.info("SynonymEnricher: enriched table %s", table.name)
            except Exception as exc:
                log.warning("SynonymEnricher: skipping %s — %s", table.name, exc)
                enriched_tables.append(table)

        return model.model_copy(update={"tables": enriched_tables})
