"""
Unit tests for weaver.enricher — no Snowflake connection required.

SynonymEnricher calls Cortex once per table and applies the returned JSON
to fill in description and synonyms fields. What is tested:

  _build_prompt        — prompt contains table name and column names
  _parse_response      — extracts JSON from raw Cortex output
  _apply_enrichment    — merges Cortex data onto SemanticTable
  SynonymEnricher.enrich — end-to-end, graceful skip on failure
"""

import json
from unittest.mock import MagicMock, patch

import pytest

from weaver.dsl import (
    BaseTable,
    DataType,
    Dimension,
    Measure,
    SemanticModel,
    SemanticTable,
    TimeDimension,
)


def _base_table(name="T"):
    return BaseTable(database="DB", schema_="SCH", table=name)


def _table(name="T", dimensions=None, time_dimensions=None, measures=None):
    return SemanticTable(
        name=name,
        base_table=_base_table(name),
        dimensions=dimensions or [],
        time_dimensions=time_dimensions or [],
        measures=measures or [],
    )


def _dim(name, dtype=DataType.VARCHAR, samples=None):
    return Dimension(name=name, expr=name, data_type=dtype, sample_values=samples or [])


def _measure(name):
    return Measure(name=name, expr=f"SUM({name})", data_type=DataType.NUMBER)


def _time_dim(name):
    return TimeDimension(name=name, expr=name, data_type=DataType.DATE)


def _model(*tables):
    return SemanticModel(name="test_model", tables=list(tables))


class TestBuildPrompt:
    def test_prompt_contains_table_name(self):
        from weaver.enricher import _build_prompt
        t = _table("ORDERS")
        assert "ORDERS" in _build_prompt(t)

    def test_prompt_contains_column_names(self):
        from weaver.enricher import _build_prompt
        t = _table("T", dimensions=[_dim("ISU_CD")], measures=[_measure("TRD_AMT")])
        prompt = _build_prompt(t)
        assert "ISU_CD" in prompt
        assert "TRD_AMT" in prompt

    def test_prompt_contains_sample_values(self):
        from weaver.enricher import _build_prompt
        t = _table("T", dimensions=[_dim("MKT_ID", samples=["STK", "KSQ"])])
        assert "STK" in _build_prompt(t)

    def test_prompt_contains_json_schema_hint(self):
        from weaver.enricher import _build_prompt
        t = _table("T")
        assert "table_description" in _build_prompt(t)
        assert "synonyms" in _build_prompt(t)


class TestParseResponse:
    def test_extracts_json_from_plain_response(self):
        from weaver.enricher import _parse_response
        raw = json.dumps({"table_description": "desc", "columns": {}})
        result = _parse_response(raw)
        assert result["table_description"] == "desc"

    def test_extracts_json_embedded_in_prose(self):
        from weaver.enricher import _parse_response
        raw = 'Here is the result:\n{"table_description": "desc", "columns": {}}\nDone.'
        result = _parse_response(raw)
        assert result["table_description"] == "desc"

    def test_raises_on_no_json(self):
        from weaver.enricher import _parse_response
        with pytest.raises(ValueError, match="no JSON object found"):
            _parse_response("no json here at all")

    def test_raises_on_invalid_json(self):
        from weaver.enricher import _parse_response
        with pytest.raises(Exception):
            _parse_response("{broken json")


class TestApplyEnrichment:
    def test_applies_table_description(self):
        from weaver.enricher import _apply_enrichment
        t = _table("T")
        data = {"table_description": "Order records", "columns": {}}
        result = _apply_enrichment(t, data)
        assert result.description == "Order records"

    def test_applies_column_description(self):
        from weaver.enricher import _apply_enrichment
        t = _table("T", dimensions=[_dim("ISU_CD")])
        data = {"table_description": "", "columns": {"ISU_CD": {"description": "Issue code", "synonyms": []}}}
        result = _apply_enrichment(t, data)
        assert result.dimensions[0].description == "Issue code"

    def test_applies_column_synonyms(self):
        from weaver.enricher import _apply_enrichment
        t = _table("T", dimensions=[_dim("ISU_CD")])
        data = {"table_description": "", "columns": {"ISU_CD": {"description": "", "synonyms": ["ticker", "stock code"]}}}
        result = _apply_enrichment(t, data)
        assert result.dimensions[0].synonyms == ["ticker", "stock code"]

    def test_preserves_existing_description_when_cortex_returns_empty(self):
        from weaver.enricher import _apply_enrichment
        dim = Dimension(name="ISU_CD", expr="ISU_CD", data_type=DataType.VARCHAR, description="existing")
        t = _table("T", dimensions=[dim])
        data = {"table_description": "", "columns": {"ISU_CD": {"description": "", "synonyms": []}}}
        result = _apply_enrichment(t, data)
        assert result.dimensions[0].description == "existing"

    def test_enriches_time_dimensions(self):
        from weaver.enricher import _apply_enrichment
        t = _table("T", time_dimensions=[_time_dim("TRD_DT")])
        data = {"table_description": "", "columns": {"TRD_DT": {"description": "Trade date", "synonyms": ["date"]}}}
        result = _apply_enrichment(t, data)
        assert result.time_dimensions[0].description == "Trade date"
        assert "date" in result.time_dimensions[0].synonyms

    def test_enriches_measures(self):
        from weaver.enricher import _apply_enrichment
        t = _table("T", measures=[_measure("TRD_AMT")])
        data = {"table_description": "", "columns": {"TRD_AMT": {"description": "Trade amount", "synonyms": ["volume"]}}}
        result = _apply_enrichment(t, data)
        assert result.measures[0].description == "Trade amount"

    def test_unknown_column_in_cortex_response_ignored(self):
        from weaver.enricher import _apply_enrichment
        t = _table("T", dimensions=[_dim("ISU_CD")])
        data = {"table_description": "", "columns": {"GHOST_COL": {"description": "does not exist", "synonyms": []}}}
        result = _apply_enrichment(t, data)
        assert result.dimensions[0].description == ""

    def test_original_model_is_not_mutated(self):
        from weaver.enricher import _apply_enrichment
        t = _table("T", dimensions=[_dim("ISU_CD")])
        original_desc = t.dimensions[0].description
        data = {"table_description": "new", "columns": {"ISU_CD": {"description": "new desc", "synonyms": []}}}
        _apply_enrichment(t, data)
        assert t.dimensions[0].description == original_desc


class TestSynonymEnricherEnrich:
    def _mock_session(self, response: str):
        session = MagicMock()
        session.sql.return_value.collect.return_value = [[response]]
        return session

    def test_returns_semantic_model(self):
        from weaver.enricher import SynonymEnricher
        response = json.dumps({"table_description": "desc", "columns": {}})
        model = _model(_table("T", dimensions=[_dim("A")]))
        result = SynonymEnricher(self._mock_session(response)).enrich(model)
        from weaver.dsl import SemanticModel
        assert isinstance(result, SemanticModel)

    def test_calls_cortex_once_per_table(self):
        from weaver.enricher import SynonymEnricher
        response = json.dumps({"table_description": "", "columns": {}})
        session = self._mock_session(response)
        model = _model(_table("T1"), _table("T2"))
        SynonymEnricher(session).enrich(model)
        assert session.sql.call_count == 2

    def test_enrichment_applied_to_output(self):
        from weaver.enricher import SynonymEnricher
        response = json.dumps({"table_description": "Trade data", "columns": {}})
        model = _model(_table("T"))
        result = SynonymEnricher(self._mock_session(response)).enrich(model)
        assert result.tables[0].description == "Trade data"

    def test_failed_table_is_left_unchanged(self):
        from weaver.enricher import SynonymEnricher
        session = MagicMock()
        session.sql.side_effect = RuntimeError("Cortex unavailable")
        model = _model(_table("T", dimensions=[_dim("A")]))
        result = SynonymEnricher(session).enrich(model)
        assert result.tables[0].description == ""

    def test_partial_failure_does_not_abort_other_tables(self):
        from weaver.enricher import SynonymEnricher
        session = MagicMock()
        good_response = json.dumps({"table_description": "Good", "columns": {}})
        session.sql.return_value.collect.side_effect = [
            RuntimeError("fail"),
            [[good_response]],
        ]
        model = _model(_table("T1"), _table("T2"))
        result = SynonymEnricher(session).enrich(model)
        assert result.tables[0].description == ""
        assert result.tables[1].description == "Good"

    def test_original_model_not_mutated(self):
        from weaver.enricher import SynonymEnricher
        response = json.dumps({"table_description": "new desc", "columns": {}})
        model = _model(_table("T"))
        original_desc = model.tables[0].description
        SynonymEnricher(self._mock_session(response)).enrich(model)
        assert model.tables[0].description == original_desc
