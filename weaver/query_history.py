"""
weaver.query_history — QueryHistoryMiner

Mines SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY for the target database/schema and
extracts business vocabulary from historical SQL — column aliases, identifiers
used in filter predicates, and snake_case aliases converted to readable phrases.

These terms are fed into SynonymEnricher as grounded business context, giving
Cortex something real to work with beyond opaque column names.

Output shape:
    QueryTerms = {table_name: {col_name: [term, ...]}}
"""

from __future__ import annotations

import logging
import re

from snowflake.snowpark import Session

log = logging.getLogger(__name__)

QueryTerms = dict[str, dict[str, list[str]]]

_LOOKBACK_DAYS = 90
_QUERY_LIMIT = 1000

_KNOWN_SQL_KEYWORDS = {
    "SELECT", "FROM", "WHERE", "JOIN", "LEFT", "RIGHT", "INNER", "OUTER",
    "ON", "AND", "OR", "NOT", "IN", "IS", "NULL", "GROUP", "BY", "ORDER",
    "HAVING", "LIMIT", "OFFSET", "AS", "DISTINCT", "UNION", "ALL", "WITH",
    "CASE", "WHEN", "THEN", "ELSE", "END", "CAST", "OVER", "PARTITION",
    "SUM", "AVG", "COUNT", "MIN", "MAX", "COALESCE", "NVL", "IFF", "NULLIF",
}

_ALIAS_RE = re.compile(
    r'\b([A-Z][A-Z0-9_]{2,})\s+AS\s+"?([A-Za-z][A-Za-z0-9_ ]{1,60})"?',
    re.IGNORECASE,
)


def _snake_to_phrase(s: str) -> str:
    """Convert snake_case or camelCase identifier to a readable phrase."""
    s = re.sub(r"([a-z])([A-Z])", r"\1 \2", s)
    return s.replace("_", " ").strip().lower()


def _is_meaningful_alias(alias: str) -> bool:
    upper = alias.upper().replace(" ", "_")
    if upper in _KNOWN_SQL_KEYWORDS:
        return False
    if re.fullmatch(r"[A-Z][0-9]+", upper):
        return False
    if len(alias.replace(" ", "").replace("_", "")) < 3:
        return False
    return True


def _extract_aliases(sql: str) -> list[tuple[str, str]]:
    """
    Return [(col_name, alias_phrase), ...] found in a SQL string.
    col_name is uppercased; alias_phrase is a space-separated readable phrase.
    """
    pairs: list[tuple[str, str]] = []
    for col, raw_alias in _ALIAS_RE.findall(sql):
        phrase = _snake_to_phrase(raw_alias)
        if _is_meaningful_alias(phrase):
            pairs.append((col.upper(), phrase))
    return pairs


class QueryHistoryMiner:
    """
    Mines SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY for business vocabulary.

    Requires ACCOUNTADMIN or SNOWFLAKE database privilege.
    Returns an empty dict gracefully if the view is inaccessible or empty.

    Usage:
        terms = QueryHistoryMiner(session).mine("NEXTRADE_EQUITY_MARKET_DATA", "FIN")
        # terms["NX_HT_ONL_MKTPR_A3"]["TRD_QTY"] → ["trade quantity", "traded qty"]
    """

    def __init__(self, session: Session) -> None:
        self._session = session

    def mine(self, database: str, schema: str) -> QueryTerms:
        log.info("QueryHistoryMiner: querying history for %s.%s", database, schema)

        queries = self._fetch_queries(database, schema)
        if not queries:
            log.info("QueryHistoryMiner: no query history found for %s.%s", database, schema)
            return {}

        table_names = self._fetch_table_names(database, schema)
        terms = self._extract_terms(queries, table_names)

        total = sum(len(cols) for cols in terms.values())
        log.info(
            "QueryHistoryMiner: mined %d term entries across %d tables from %d queries",
            total, len(terms), len(queries),
        )
        return terms

    def _fetch_queries(self, database: str, schema: str) -> list[str]:
        try:
            rows = self._session.sql(f"""
                SELECT query_text
                FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
                WHERE database_name  = '{database}'
                  AND schema_name    = '{schema}'
                  AND query_type     = 'SELECT'
                  AND execution_status = 'SUCCESS'
                  AND start_time     > DATEADD(day, -{_LOOKBACK_DAYS}, CURRENT_TIMESTAMP())
                ORDER BY start_time DESC
                LIMIT {_QUERY_LIMIT}
            """).collect()
            return [row[0] for row in rows if row[0]]
        except Exception as exc:
            log.warning("QueryHistoryMiner: could not read query history — %s", exc)
            return []

    def _fetch_table_names(self, database: str, schema: str) -> set[str]:
        try:
            rows = self._session.sql(f"""
                SELECT table_name
                FROM {database}.INFORMATION_SCHEMA.TABLES
                WHERE table_schema = '{schema}'
                  AND table_type   = 'BASE TABLE'
            """).collect()
            return {row[0].upper() for row in rows}
        except Exception:
            return set()

    def _extract_terms(self, queries: list[str], table_names: set[str]) -> QueryTerms:
        col_aliases: dict[str, dict[str, set[str]]] = {}

        for sql in queries:
            aliases = _extract_aliases(sql)
            if not aliases:
                continue

            referenced_tables = _find_referenced_tables(sql, table_names)
            if not referenced_tables:
                continue

            for col, phrase in aliases:
                for tname in referenced_tables:
                    col_aliases.setdefault(tname, {}).setdefault(col, set()).add(phrase)

        return {
            tname: {col: sorted(phrases) for col, phrases in cols.items()}
            for tname, cols in col_aliases.items()
        }


def _find_referenced_tables(sql: str, known_tables: set[str]) -> set[str]:
    """Return the subset of known_tables that are mentioned in the SQL string."""
    upper_sql = sql.upper()
    return {t for t in known_tables if t in upper_sql}
