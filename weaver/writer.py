"""
weaver.writer — YAMLWriter

Programmatically builds a Cortex Analyst SemanticModel from a SchemaProfile.
No LLM calls. Output is always structurally valid and ready for the evaluation loop.

Column classification:
  DATE / TIMESTAMP*           → time_dimensions
  VARCHAR / BOOLEAN           → dimensions
  NUMBER / FLOAT (categorical) → dimensions  (name ends with _CD/_ID/_NO/… or is an FK key)
  NUMBER / FLOAT (metric)     → measures     (SUM/AVG chosen by name suffix)
  VARIANT / OBJECT / ARRAY    → skipped

Synonym and description enrichment is a separate pipeline stage (SynonymEnricher)
that calls Cortex on the already-valid model.
"""

from __future__ import annotations

import logging

from snowflake.snowpark import Session

from weaver.discovery import SchemaProfile, TableProfile
from weaver.dsl import (
    BaseTable,
    DataType,
    Dimension,
    JoinType,
    Measure,
    PrimaryKey,
    Relationship,
    RelationshipColumn,
    RelationshipType,
    SemanticModel,
    SemanticTable,
    TimeDimension,
)

log = logging.getLogger(__name__)

_TEMPORAL_TYPES = {"DATE", "TIMESTAMP_NTZ", "TIMESTAMP_LTZ", "TIMESTAMP_TZ"}
_SKIP_TYPES = {"VARIANT", "OBJECT", "ARRAY"}

_CATEGORICAL_SUFFIXES = (
    "_CD", "_ID", "_NO", "_TP", "_GRP", "_YN", "_FG", "_GB",
    "_CL", "_DIV", "_SEQ", "_KEY", "_CODE",
)
_SUM_SUFFIXES = ("_AMT", "_VAL", "_QTY", "_CNT", "_CT", "_TOT", "_SUM", "_VOL")
_AVG_SUFFIXES = ("_PRC", "PRC", "_PRICE", "_RATE", "_RATIO", "_PCT", "_AVG")


class YAMLWriter:
    """
    Builds a Cortex Analyst SemanticModel from a SchemaProfile programmatically.

    Usage:
        model = YAMLWriter(session).generate(profile)
    """

    def __init__(self, session: Session) -> None:
        self._session = session

    def generate(self, profile: SchemaProfile) -> SemanticModel:
        fk_columns = _collect_fk_columns(profile)
        pk_map = _infer_primary_keys(profile)
        tables = [_build_table(t, profile, fk_columns, pk_map) for t in profile["tables"]]
        relationships = _build_relationships(tables, profile)

        db = profile["database"].lower()
        sch = profile["schema"].lower()

        log.info(
            "YAMLWriter: built %d tables, %d relationships",
            len(tables), len(relationships),
        )
        return SemanticModel(
            name=f"{db}_{sch}",
            description=f"Semantic model for {profile['database']}.{profile['schema']}",
            tables=tables,
            relationships=relationships,
        )


def _infer_primary_keys(profile: SchemaProfile) -> dict[str, set[str]]:
    """Return {table_name: {pk_col, ...}} inferred from FK candidates.

    For each FK candidate T1.col → T2.col, T2's col is treated as its PK.
    Tables that are only on the left side (fact tables) get no PK.
    """
    pk_map: dict[str, set[str]] = {}
    for table in profile["tables"]:
        for cand in table["fk_candidates"]:
            for match in cand["matches"]:
                right_table, right_col = match.split(".", 1)
                pk_map.setdefault(right_table, set()).add(right_col)
    return pk_map


def _collect_fk_columns(profile: SchemaProfile) -> set[tuple[str, str]]:
    """Return a set of (table_name, col_name) pairs that appear in any fk_candidates list."""
    fk: set[tuple[str, str]] = set()
    for table in profile["tables"]:
        for cand in table["fk_candidates"]:
            fk.add((table["name"], cand["column"]))
    return fk


def _build_table(
    t: TableProfile,
    profile: SchemaProfile,
    fk_columns: set[tuple[str, str]],
    pk_map: dict[str, set[str]] | None = None,
) -> SemanticTable:
    dimensions: list[Dimension] = []
    time_dimensions: list[TimeDimension] = []
    measures: list[Measure] = []

    for col in t["columns"]:
        dtype = col["type"]
        cname = col["name"]
        comment = col["comment"]

        if dtype in _SKIP_TYPES:
            continue

        if dtype in _TEMPORAL_TYPES:
            time_dimensions.append(TimeDimension(
                name=cname,
                expr=cname,
                data_type=DataType(dtype),
                description=comment,
            ))

        elif dtype in {"VARCHAR", "BOOLEAN"}:
            dimensions.append(Dimension(
                name=cname,
                expr=cname,
                data_type=DataType(dtype),
                description=comment,
                sample_values=col["sample_values"],
            ))

        else:
            is_fk = (t["name"], cname) in fk_columns
            is_categorical = cname.upper().endswith(_CATEGORICAL_SUFFIXES)

            if is_fk or is_categorical:
                dimensions.append(Dimension(
                    name=cname,
                    expr=cname,
                    data_type=DataType(dtype),
                    description=comment,
                    sample_values=col["sample_values"],
                ))
            else:
                agg = _pick_aggregation(cname)
                measures.append(Measure(
                    name=cname,
                    expr=f"{agg}({cname})",
                    data_type=DataType(dtype),
                    description=comment,
                ))

    pk_cols = sorted((pk_map or {}).get(t["name"], set()))
    primary_key = PrimaryKey(columns=pk_cols) if pk_cols else None

    return SemanticTable(
        name=t["name"],
        description=t["comment"],
        base_table=BaseTable(
            database=profile["database"],
            schema_=profile["schema"],
            table=t["name"],
        ),
        primary_key=primary_key,
        dimensions=dimensions,
        time_dimensions=time_dimensions,
        measures=measures,
    )


def _pick_aggregation(col_name: str) -> str:
    upper = col_name.upper()
    if upper.endswith(_AVG_SUFFIXES):
        return "AVG"
    return "SUM"


def _build_relationships(
    tables: list[SemanticTable],
    profile: SchemaProfile,
) -> list[Relationship]:
    table_names = {t.name for t in tables}
    seen: set[frozenset] = set()
    relationships: list[Relationship] = []

    for t in profile["tables"]:
        for cand in t["fk_candidates"]:
            col = cand["column"]
            for match in cand["matches"]:
                other_name = match.split(".")[0]
                if other_name not in table_names:
                    continue

                pair = frozenset([t["name"], other_name])
                if pair in seen:
                    continue
                seen.add(pair)

                relationships.append(Relationship(
                    name=f"{t['name']}_to_{other_name}",
                    left_table=t["name"],
                    right_table=other_name,
                    relationship_type=RelationshipType.MANY_TO_ONE,
                    join_type=JoinType.LEFT_OUTER,
                    relationship_columns=[
                        RelationshipColumn(left_column=col, right_column=col)
                    ],
                ))

    return relationships
