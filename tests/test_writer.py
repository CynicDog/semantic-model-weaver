"""
Unit tests for weaver.writer — no Snowflake connection required.

YAMLWriter is fully programmatic (no LLM calls). What is tested:

  _collect_fk_columns    — builds the set of (table, col) FK pairs
  _pick_aggregation      — SUM vs AVG selection by column name suffix
  _build_table           — column → time_dimension / dimension / measure classification
  _build_relationships   — FK candidates → Relationship objects, deduplication
  YAMLWriter.generate    — end-to-end SemanticModel assembly
"""

from unittest.mock import MagicMock

import pytest

from weaver.dsl import DataType


def _col(name, type_, nullable=True, comment="", sample_values=None):
    return {"name": name, "type": type_, "nullable": nullable,
            "comment": comment, "sample_values": sample_values or []}


def _table(name, columns, comment="", row_count=0, fk_candidates=None):
    return {"name": name, "comment": comment, "row_count": row_count,
            "columns": columns, "fk_candidates": fk_candidates or []}


def _profile(*tables):
    return {"database": "MYDB", "schema": "MYSCH", "tables": list(tables)}


class TestCollectFkColumns:
    def test_returns_table_col_pairs(self):
        from weaver.writer import _collect_fk_columns
        profile = _profile(
            _table("T1", [], fk_candidates=[{"column": "ISU_CD", "matches": ["T2.ISU_CD"]}])
        )
        assert ("T1", "ISU_CD") in _collect_fk_columns(profile)

    def test_empty_when_no_fk_candidates(self):
        from weaver.writer import _collect_fk_columns
        profile = _profile(_table("T1", []))
        assert _collect_fk_columns(profile) == set()

    def test_collects_across_multiple_tables(self):
        from weaver.writer import _collect_fk_columns
        profile = _profile(
            _table("T1", [], fk_candidates=[{"column": "A", "matches": ["T2.A"]}]),
            _table("T2", [], fk_candidates=[{"column": "B", "matches": ["T1.B"]}]),
        )
        fks = _collect_fk_columns(profile)
        assert ("T1", "A") in fks
        assert ("T2", "B") in fks


class TestPickAggregation:
    def test_avg_for_price_suffix(self):
        from weaver.writer import _pick_aggregation
        assert _pick_aggregation("CLSPRC") == "AVG"

    def test_avg_for_rate_suffix(self):
        from weaver.writer import _pick_aggregation
        assert _pick_aggregation("INT_RATE") == "AVG"

    def test_sum_for_amount_suffix(self):
        from weaver.writer import _pick_aggregation
        assert _pick_aggregation("TRD_AMT") == "SUM"

    def test_sum_for_volume_suffix(self):
        from weaver.writer import _pick_aggregation
        assert _pick_aggregation("TRD_VOL") == "SUM"

    def test_sum_is_default(self):
        from weaver.writer import _pick_aggregation
        assert _pick_aggregation("UNKNOWN_METRIC") == "SUM"


class TestBuildTable:
    def _run(self, table, profile=None, fk_columns=None):
        from weaver.writer import _build_table
        profile = profile or _profile(table)
        return _build_table(table, profile, fk_columns or set())

    def test_date_column_becomes_time_dimension(self):
        t = _table("T", [_col("TRD_DT", "DATE")])
        result = self._run(t)
        assert len(result.time_dimensions) == 1
        assert result.time_dimensions[0].name == "TRD_DT"

    def test_timestamp_column_becomes_time_dimension(self):
        t = _table("T", [_col("TS", "TIMESTAMP_NTZ")])
        result = self._run(t)
        assert len(result.time_dimensions) == 1

    def test_varchar_column_becomes_dimension(self):
        t = _table("T", [_col("ISU_CD", "VARCHAR")])
        result = self._run(t)
        assert len(result.dimensions) == 1
        assert result.dimensions[0].name == "ISU_CD"

    def test_boolean_column_becomes_dimension(self):
        t = _table("T", [_col("IS_ACTIVE", "BOOLEAN")])
        result = self._run(t)
        assert len(result.dimensions) == 1

    def test_categorical_number_becomes_dimension(self):
        t = _table("T", [_col("MKT_ID", "NUMBER")])
        result = self._run(t)
        assert any(d.name == "MKT_ID" for d in result.dimensions)
        assert not result.measures

    def test_metric_number_becomes_measure(self):
        t = _table("T", [_col("TRD_AMT", "NUMBER")])
        result = self._run(t)
        assert len(result.measures) == 1
        assert result.measures[0].name == "TRD_AMT"

    def test_measure_expr_contains_aggregation(self):
        t = _table("T", [_col("TRD_AMT", "NUMBER")])
        result = self._run(t)
        assert "(" in result.measures[0].expr
        assert "TRD_AMT" in result.measures[0].expr

    def test_fk_column_forced_to_dimension_even_if_numeric(self):
        t = _table("T", [_col("ORDER_NUM", "NUMBER")])
        fk_columns = {("T", "ORDER_NUM")}
        result = self._run(t, fk_columns=fk_columns)
        assert any(d.name == "ORDER_NUM" for d in result.dimensions)
        assert not result.measures

    def test_variant_column_skipped(self):
        t = _table("T", [_col("META", "VARIANT")])
        result = self._run(t)
        assert not result.dimensions
        assert not result.measures
        assert not result.time_dimensions

    def test_sample_values_preserved_on_dimension(self):
        t = _table("T", [_col("MKT_ID", "VARCHAR", sample_values=["STK", "KSQ"])])
        result = self._run(t)
        assert result.dimensions[0].sample_values == ["STK", "KSQ"]

    def test_base_table_points_to_physical_table(self):
        t = _table("MY_TABLE", [])
        result = self._run(t, profile=_profile(t))
        assert result.base_table.table == "MY_TABLE"
        assert result.base_table.database == "MYDB"
        assert result.base_table.schema_ == "MYSCH"

    def test_avg_aggregation_for_price_column(self):
        t = _table("T", [_col("CLSPRC", "FLOAT")])
        result = self._run(t)
        assert result.measures[0].expr.startswith("AVG(")


class TestBuildRelationships:
    def _run(self, profile):
        from weaver.writer import _build_relationships
        from weaver.writer import _collect_fk_columns, _build_table
        fk_columns = _collect_fk_columns(profile)
        tables = [_build_table(t, profile, fk_columns) for t in profile["tables"]]
        return _build_relationships(tables, profile), tables

    def test_fk_candidate_creates_relationship(self):
        profile = _profile(
            _table("T1", [_col("ISU_CD", "VARCHAR")],
                   fk_candidates=[{"column": "ISU_CD", "matches": ["T2.ISU_CD"]}]),
            _table("T2", [_col("ISU_CD", "VARCHAR")]),
        )
        rels, _ = self._run(profile)
        assert len(rels) == 1

    def test_relationship_references_correct_tables(self):
        profile = _profile(
            _table("T1", [_col("ISU_CD", "VARCHAR")],
                   fk_candidates=[{"column": "ISU_CD", "matches": ["T2.ISU_CD"]}]),
            _table("T2", [_col("ISU_CD", "VARCHAR")]),
        )
        rels, _ = self._run(profile)
        assert rels[0].left_table == "T1"
        assert rels[0].right_table == "T2"

    def test_no_duplicate_relationships_for_same_pair(self):
        profile = _profile(
            _table("T1", [_col("ISU_CD", "VARCHAR")],
                   fk_candidates=[{"column": "ISU_CD", "matches": ["T2.ISU_CD", "T2.ISU_CD"]}]),
            _table("T2", [_col("ISU_CD", "VARCHAR")]),
        )
        rels, _ = self._run(profile)
        assert len(rels) == 1

    def test_unknown_table_in_match_is_skipped(self):
        profile = _profile(
            _table("T1", [_col("ISU_CD", "VARCHAR")],
                   fk_candidates=[{"column": "ISU_CD", "matches": ["GHOST.ISU_CD"]}]),
        )
        rels, _ = self._run(profile)
        assert rels == []

    def test_relationship_column_uses_fk_column_name(self):
        profile = _profile(
            _table("T1", [_col("ISU_CD", "VARCHAR")],
                   fk_candidates=[{"column": "ISU_CD", "matches": ["T2.ISU_CD"]}]),
            _table("T2", [_col("ISU_CD", "VARCHAR")]),
        )
        rels, _ = self._run(profile)
        rc = rels[0].relationship_columns[0]
        assert rc.left_column == "ISU_CD"
        assert rc.right_column == "ISU_CD"


class TestYAMLWriterGenerate:
    def test_returns_semantic_model(self):
        from weaver.writer import YAMLWriter
        profile = _profile(_table("T1", [_col("ID", "VARCHAR")]))
        model = YAMLWriter(MagicMock()).generate(profile)
        from weaver.dsl import SemanticModel
        assert isinstance(model, SemanticModel)

    def test_model_name_derived_from_database_and_schema(self):
        from weaver.writer import YAMLWriter
        profile = _profile(_table("T1", []))
        model = YAMLWriter(MagicMock()).generate(profile)
        assert "mydb" in model.name
        assert "mysch" in model.name

    def test_table_count_matches_profile(self):
        from weaver.writer import YAMLWriter
        profile = _profile(
            _table("T1", [_col("A", "VARCHAR")]),
            _table("T2", [_col("B", "DATE")]),
        )
        model = YAMLWriter(MagicMock()).generate(profile)
        assert len(model.tables) == 2

    def test_relationships_included_when_fk_candidates_exist(self):
        from weaver.writer import YAMLWriter
        profile = _profile(
            _table("T1", [_col("ISU_CD", "VARCHAR")],
                   fk_candidates=[{"column": "ISU_CD", "matches": ["T2.ISU_CD"]}]),
            _table("T2", [_col("ISU_CD", "VARCHAR")]),
        )
        model = YAMLWriter(MagicMock()).generate(profile)
        assert len(model.relationships) == 1

    def test_model_is_pydantic_valid(self):
        from weaver.writer import YAMLWriter
        profile = _profile(
            _table("T1", [_col("ISU_CD", "VARCHAR")],
                   fk_candidates=[{"column": "ISU_CD", "matches": ["T2.ISU_CD"]}]),
            _table("T2", [_col("ISU_CD", "VARCHAR"), _col("TRD_AMT", "NUMBER")]),
        )
        model = YAMLWriter(MagicMock()).generate(profile)
        reparsed = model.from_yaml(model.to_yaml())
        assert reparsed.name == model.name

    def test_session_not_used_during_generate(self):
        from weaver.writer import YAMLWriter
        profile = _profile(_table("T1", [_col("A", "VARCHAR")]))
        session = MagicMock()
        YAMLWriter(session).generate(profile)
        session.sql.assert_not_called()
