"""
weaver.discovery — SchemaDiscovery

Reads INFORMATION_SCHEMA via Snowpark and produces a SchemaProfile dict that
every downstream stage uses as its source of truth.

Output shape
------------
SchemaProfile = {
    "database": str,
    "schema": str,
    "tables": [
        {
            "name": str,
            "comment": str,
            "row_count": int,
            "columns": [
                {
                    "name": str,
                    "type": str,          # normalised to DSL DataType values
                    "nullable": bool,
                    "comment": str,
                    "sample_values": list[str],  # non-empty only for text/bool columns
                }
            ],
            "fk_candidates": [
                {"column": str, "matches": ["OtherTable.COL", ...]}
            ],
        }
    ],
}
"""

from __future__ import annotations

import logging
from typing import TypedDict

from snowflake.snowpark import Session
from snowflake.snowpark import functions as F

log = logging.getLogger(__name__)

_SAMPLE_ROWS = 100
_MAX_SAMPLE_VALUES = 5

# Snowflake INFORMATION_SCHEMA DATA_TYPE values that are worth sampling
# (text-like and boolean — numeric/temporal values aren't useful as dimension hints)
_SAMPLEABLE_BASE_TYPES = {"TEXT", "VARCHAR", "STRING", "CHAR", "CHARACTER", "BOOLEAN"}

# Snowflake internal type → DSL DataType string
_SF_TYPE_MAP: dict[str, str] = {
    "TEXT": "VARCHAR",
    "VARCHAR": "VARCHAR",
    "STRING": "VARCHAR",
    "CHAR": "VARCHAR",
    "CHARACTER": "VARCHAR",
    "FIXED": "NUMBER",
    "NUMBER": "NUMBER",
    "NUMERIC": "NUMBER",
    "DECIMAL": "NUMBER",
    "INTEGER": "NUMBER",
    "INT": "NUMBER",
    "BIGINT": "NUMBER",
    "SMALLINT": "NUMBER",
    "TINYINT": "NUMBER",
    "BYTEINT": "NUMBER",
    "REAL": "FLOAT",
    "FLOAT": "FLOAT",
    "FLOAT4": "FLOAT",
    "FLOAT8": "FLOAT",
    "DOUBLE": "FLOAT",
    "DOUBLE PRECISION": "FLOAT",
    "DATE": "DATE",
    "TIMESTAMP_NTZ": "TIMESTAMP_NTZ",
    "TIMESTAMP_LTZ": "TIMESTAMP_LTZ",
    "TIMESTAMP_TZ": "TIMESTAMP_TZ",
    "TIMESTAMP": "TIMESTAMP_NTZ",
    "DATETIME": "TIMESTAMP_NTZ",
    "BOOLEAN": "BOOLEAN",
    "VARIANT": "VARIANT",
    "OBJECT": "OBJECT",
    "ARRAY": "ARRAY",
}


class ColumnProfile(TypedDict):
    name: str
    type: str
    nullable: bool
    comment: str
    sample_values: list[str]


class FKCandidate(TypedDict):
    column: str
    matches: list[str]


class TableProfile(TypedDict):
    name: str
    comment: str
    row_count: int
    columns: list[ColumnProfile]
    fk_candidates: list[FKCandidate]


class SchemaProfile(TypedDict):
    database: str
    schema: str
    tables: list[TableProfile]


class SchemaDiscovery:
    """
    Profiles a Snowflake database schema and returns a SchemaProfile.

    Usage:
        profile = SchemaDiscovery(session).run("NEXTRADE_EQUITY_MARKET_DATA", "FIN")
    """

    def __init__(self, session: Session) -> None:
        self.session = session

    def run(self, database: str, schema: str) -> SchemaProfile:
        log.info("SchemaDiscovery: profiling %s.%s", database, schema)

        tables_meta = self._fetch_tables(database, schema)
        columns_meta = self._fetch_columns(database, schema)

        columns_by_table: dict[str, list[dict]] = {}
        for col in columns_meta:
            columns_by_table.setdefault(col["table_name"], []).append(col)

        table_profiles: list[TableProfile] = []
        for table in tables_meta:
            table_name = table["name"]
            raw_columns = columns_by_table.get(table_name, [])
            sample_map = self._sample_table(database, schema, table_name, raw_columns)

            columns: list[ColumnProfile] = [
                {
                    "name": col["name"],
                    "type": _normalize_type(col["raw_type"]),
                    "nullable": col["nullable"],
                    "comment": col["comment"],
                    "sample_values": sample_map.get(col["name"], []),
                }
                for col in raw_columns
            ]

            table_profiles.append({
                "name": table_name,
                "comment": table["comment"],
                "row_count": table["row_count"],
                "columns": columns,
                "fk_candidates": [],
            })

        _infer_fk_candidates(table_profiles)

        log.info(
            "SchemaDiscovery: found %d tables in %s.%s",
            len(table_profiles), database, schema,
        )
        return {"database": database, "schema": schema, "tables": table_profiles}

    def _fetch_tables(self, database: str, schema: str) -> list[dict]:
        rows = self.session.sql(f"""
            SELECT
                table_name,
                COALESCE(comment, '')  AS comment,
                COALESCE(row_count, 0) AS row_count
            FROM {database}.INFORMATION_SCHEMA.TABLES
            WHERE table_schema = '{schema}'
              AND table_type   IN ('BASE TABLE', 'VIEW')
            ORDER BY table_name
        """).collect()
        return [
            {
                "name": row["TABLE_NAME"],
                "comment": row["COMMENT"] or "",
                "row_count": int(row["ROW_COUNT"] or 0),
            }
            for row in rows
        ]

    def _fetch_columns(self, database: str, schema: str) -> list[dict]:
        rows = self.session.sql(f"""
            SELECT
                table_name,
                column_name,
                data_type,
                is_nullable,
                COALESCE(comment, '') AS comment
            FROM {database}.INFORMATION_SCHEMA.COLUMNS
            WHERE table_schema = '{schema}'
            ORDER BY table_name, ordinal_position
        """).collect()
        return [
            {
                "table_name": row["TABLE_NAME"],
                "name": row["COLUMN_NAME"],
                "raw_type": row["DATA_TYPE"],
                "nullable": row["IS_NULLABLE"] == "YES",
                "comment": row["COMMENT"] or "",
            }
            for row in rows
        ]

    def _sample_table(
        self,
        database: str,
        schema: str,
        table_name: str,
        columns: list[dict],
    ) -> dict[str, list[str]]:
        """Return {column_name: [sample_value, ...]} for text/boolean columns."""
        sampleable = [
            col for col in columns
            if col["raw_type"].upper().split("(")[0].strip() in _SAMPLEABLE_BASE_TYPES
        ]
        if not sampleable:
            return {}

        try:
            select_exprs = [F.col(f'"{col["name"]}"') for col in sampleable]
            rows = (
                self.session.table(f"{database}.{schema}.{table_name}")
                .select(*select_exprs)
                .sample(n=_SAMPLE_ROWS)
                .collect()
            )
        except Exception as exc:
            log.warning(
                "Could not sample %s.%s.%s: %s", database, schema, table_name, exc
            )
            return {}

        result: dict[str, list[str]] = {}
        for col in sampleable:
            col_name = col["name"]
            seen: set[str] = set()
            values: list[str] = []
            for row in rows:
                val = row[col_name]
                if val is not None:
                    s = str(val)
                    if s not in seen and len(values) < _MAX_SAMPLE_VALUES:
                        seen.add(s)
                        values.append(s)
            result[col_name] = values

        return result


def _normalize_type(raw_type: str) -> str:
    """Map a Snowflake INFORMATION_SCHEMA DATA_TYPE to the DSL DataType string."""
    base = raw_type.upper().split("(")[0].strip()
    return _SF_TYPE_MAP.get(base, "VARCHAR")


def _type_family(raw_type: str) -> str:
    """Coarse type family used for FK candidate compatibility checks."""
    normalised = _normalize_type(raw_type)
    if normalised == "VARCHAR":
        return "text"
    if normalised in {"NUMBER", "FLOAT"}:
        return "numeric"
    if normalised == "DATE":
        return "date"
    if normalised in {"TIMESTAMP_NTZ", "TIMESTAMP_LTZ", "TIMESTAMP_TZ"}:
        return "timestamp"
    return normalised.lower()


def _infer_fk_candidates(tables: list[TableProfile]) -> None:
    """
    Populate fk_candidates in-place for every TableProfile.

    A column is a FK candidate when another table has a column with the same
    name (case-insensitive) and a compatible type family.
    """
    # index: (UPPER_NAME, type_family) → [(table_name, col_name), ...]
    index: dict[tuple[str, str], list[tuple[str, str]]] = {}
    for table in tables:
        for col in table["columns"]:
            key = (col["name"].upper(), _type_family(col["type"]))
            index.setdefault(key, []).append((table["name"], col["name"]))

    for table in tables:
        candidates: list[FKCandidate] = []
        for col in table["columns"]:
            key = (col["name"].upper(), _type_family(col["type"]))
            peers = index.get(key, [])
            matches = [
                f"{other_table}.{other_col}"
                for other_table, other_col in peers
                if other_table != table["name"]
            ]
            if matches:
                candidates.append({"column": col["name"], "matches": matches})
        table["fk_candidates"] = candidates
