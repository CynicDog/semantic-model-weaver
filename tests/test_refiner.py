"""
Unit tests for weaver.refiner — no Snowflake connection required.

  _score_summary       — computes mean scores from feedback_df
  _failed_questions    — extracts low-scoring question inputs
  _build_patch_prompt  — prompt contains column info and failed questions
  _parse_patch         — extracts patches dict from Cortex JSON
  _apply_patch         — merges synonyms/descriptions onto SemanticTable
  RefinementAgent.refine — convergence check, graceful skip, patching
"""

import json
from unittest.mock import MagicMock

import pandas as pd
import pytest

from weaver.dsl import (
    BaseTable,
    DataType,
    Dimension,
    Measure,
    SemanticModel,
    SemanticTable,
)


def _base_table(name="T"):
    return BaseTable(database="DB", schema_="SCH", table=name)


def _table(name="T", dimensions=None, measures=None, time_dimensions=None):
    return SemanticTable(
        name=name,
        base_table=_base_table(name),
        dimensions=dimensions or [],
        time_dimensions=time_dimensions or [],
        measures=measures or [],
    )


def _dim(name, desc="", synonyms=None):
    return Dimension(
        name=name, expr=name, data_type=DataType.VARCHAR,
        description=desc, synonyms=synonyms or [],
    )


def _measure(name):
    return Measure(name=name, expr=f"SUM({name})", data_type=DataType.NUMBER)


def _model(*tables):
    return SemanticModel(name="m", tables=list(tables))


def _feedback(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows)


def _mock_session(response=""):
    session = MagicMock()
    session.sql.return_value.collect.return_value = [[response]]
    return session


class TestScoreSummary:
    def test_returns_mean_per_numeric_column(self):
        from weaver.refiner import _score_summary
        df = _feedback([
            {"correctness": 0.4, "answer_relevance": 0.8},
            {"correctness": 0.6, "answer_relevance": 0.9},
        ])
        scores = _score_summary(df)
        assert abs(scores["correctness"] - 0.5) < 1e-9

    def test_returns_empty_on_empty_df(self):
        from weaver.refiner import _score_summary
        assert _score_summary(pd.DataFrame()) == {}


class TestFailedQuestions:
    def test_returns_low_scoring_inputs(self):
        from weaver.refiner import _failed_questions
        df = _feedback([
            {"input": "Q1", "correctness": 0.3},
            {"input": "Q2", "correctness": 0.9},
        ])
        failed = _failed_questions(df)
        assert "Q1" in failed
        assert "Q2" not in failed

    def test_returns_empty_on_empty_df(self):
        from weaver.refiner import _failed_questions
        assert _failed_questions(pd.DataFrame()) == []

    def test_returns_empty_when_no_correctness_column(self):
        from weaver.refiner import _failed_questions
        df = _feedback([{"input": "Q1", "answer_relevance": 0.9}])
        assert _failed_questions(df) == []


class TestBuildPatchPrompt:
    def test_contains_table_name(self):
        from weaver.refiner import _build_patch_prompt
        t = _table("ORDERS")
        assert "ORDERS" in _build_patch_prompt(t, ["bad question"])

    def test_contains_column_names(self):
        from weaver.refiner import _build_patch_prompt
        t = _table("T", dimensions=[_dim("ISU_CD")])
        assert "ISU_CD" in _build_patch_prompt(t, [])

    def test_contains_failed_questions(self):
        from weaver.refiner import _build_patch_prompt
        t = _table("T")
        assert "failed question" in _build_patch_prompt(t, ["failed question"])

    def test_json_schema_hint_present(self):
        from weaver.refiner import _build_patch_prompt
        t = _table("T")
        prompt = _build_patch_prompt(t, [])
        assert "patches" in prompt
        assert "synonyms" in prompt


class TestParsePatch:
    def test_extracts_patches_dict(self):
        from weaver.refiner import _parse_patch
        raw = json.dumps(
            {"patches": {"ISU_CD": {"description": "Issue code", "synonyms": ["ticker"]}}}
        )
        patches = _parse_patch(raw)
        assert "ISU_CD" in patches
        assert patches["ISU_CD"]["synonyms"] == ["ticker"]

    def test_raises_on_no_json(self):
        from weaver.refiner import _parse_patch
        with pytest.raises(ValueError):
            _parse_patch("no json")

    def test_empty_patches_key_returns_empty_dict(self):
        from weaver.refiner import _parse_patch
        raw = json.dumps({"patches": {}})
        assert _parse_patch(raw) == {}


class TestApplyPatch:
    def test_applies_synonyms_to_dimension(self):
        from weaver.refiner import _apply_patch
        t = _table("T", dimensions=[_dim("ISU_CD")])
        patches = {"ISU_CD": {"description": "", "synonyms": ["ticker", "stock code"]}}
        result = _apply_patch(t, patches)
        assert "ticker" in result.dimensions[0].synonyms

    def test_merges_existing_and_new_synonyms(self):
        from weaver.refiner import _apply_patch
        t = _table("T", dimensions=[_dim("ISU_CD", synonyms=["existing"])])
        patches = {"ISU_CD": {"description": "", "synonyms": ["new"]}}
        result = _apply_patch(t, patches)
        syns = result.dimensions[0].synonyms
        assert "existing" in syns
        assert "new" in syns

    def test_deduplicates_synonyms(self):
        from weaver.refiner import _apply_patch
        t = _table("T", dimensions=[_dim("ISU_CD", synonyms=["ticker"])])
        patches = {"ISU_CD": {"description": "", "synonyms": ["ticker"]}}
        result = _apply_patch(t, patches)
        assert result.dimensions[0].synonyms.count("ticker") == 1

    def test_applies_description(self):
        from weaver.refiner import _apply_patch
        t = _table("T", dimensions=[_dim("ISU_CD")])
        patches = {"ISU_CD": {"description": "Issue code", "synonyms": []}}
        result = _apply_patch(t, patches)
        assert result.dimensions[0].description == "Issue code"

    def test_does_not_mutate_original(self):
        from weaver.refiner import _apply_patch
        t = _table("T", dimensions=[_dim("ISU_CD")])
        original_desc = t.dimensions[0].description
        _apply_patch(t, {"ISU_CD": {"description": "new", "synonyms": []}})
        assert t.dimensions[0].description == original_desc

    def test_unknown_column_patch_ignored(self):
        from weaver.refiner import _apply_patch
        t = _table("T", dimensions=[_dim("ISU_CD")])
        patches = {"GHOST": {"description": "ignored", "synonyms": []}}
        result = _apply_patch(t, patches)
        assert result.dimensions[0].description == ""

    def test_patches_measures(self):
        from weaver.refiner import _apply_patch
        t = _table("T", measures=[_measure("TRD_AMT")])
        patches = {"TRD_AMT": {"description": "trade amount", "synonyms": ["volume"]}}
        result = _apply_patch(t, patches)
        assert result.measures[0].description == "trade amount"


class TestRefinementAgentRefine:
    def _good_feedback(self, correctness=0.4):
        return _feedback([
            {"input": "bad question", "correctness": correctness, "answer_relevance": 0.5}
        ])

    def test_returns_none_when_above_threshold(self):
        from weaver.refiner import RefinementAgent
        model = _model(_table("T"))
        feedback = _feedback([{"input": "Q", "correctness": 0.95}])
        result = RefinementAgent(_mock_session()).refine(model, feedback)
        assert result is None

    def test_returns_model_when_below_threshold(self):
        from weaver.refiner import RefinementAgent
        patch_response = json.dumps(
            {"patches": {"ISU_CD": {"description": "d", "synonyms": ["s"]}}}
        )
        model = _model(_table("T", dimensions=[_dim("ISU_CD")]))
        result = RefinementAgent(_mock_session(patch_response)).refine(model, self._good_feedback())
        assert isinstance(result, SemanticModel)

    def test_returns_none_on_empty_feedback(self):
        from weaver.refiner import RefinementAgent
        model = _model(_table("T"))
        result = RefinementAgent(_mock_session()).refine(model, pd.DataFrame())
        assert result is None

    def test_cortex_failure_skips_table_gracefully(self):
        from weaver.refiner import RefinementAgent
        session = MagicMock()
        session.sql.side_effect = RuntimeError("Cortex down")
        model = _model(_table("T", dimensions=[_dim("A")]))
        result = RefinementAgent(session).refine(model, self._good_feedback())
        assert isinstance(result, SemanticModel)
        assert result.tables[0].dimensions[0].synonyms == []

    def test_does_not_mutate_original_model(self):
        from weaver.refiner import RefinementAgent
        patch_response = json.dumps(
            {"patches": {"ISU_CD": {"description": "d", "synonyms": ["s"]}}}
        )
        model = _model(_table("T", dimensions=[_dim("ISU_CD")]))
        original_desc = model.tables[0].dimensions[0].description
        RefinementAgent(_mock_session(patch_response)).refine(model, self._good_feedback())
        assert model.tables[0].dimensions[0].description == original_desc
