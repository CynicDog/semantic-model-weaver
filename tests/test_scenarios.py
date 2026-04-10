"""
Unit tests for weaver.scenarios — no Snowflake connection required.

  _col_summary       — column → summary string
  _build_prompt      — prompt contains table/column info and JSON schema hint
  _parse_scenarios   — extracts scenarios list from raw Cortex JSON
  _execute_sql       — returns formatted string or None on failure
  _find_related      — finds tables sharing column names
  ScenarioGenerator  — end-to-end, graceful skip on Cortex or SQL failure
"""

import json
from unittest.mock import MagicMock

import pytest


def _col(name, type_, comment="", samples=None):
    return {"name": name, "type": type_, "nullable": True,
            "comment": comment, "sample_values": samples or []}


def _table(name, columns=None, fk_candidates=None):
    return {"name": name, "comment": "", "row_count": 0,
            "columns": columns or [], "fk_candidates": fk_candidates or []}


def _profile(*tables):
    return {"database": "DB", "schema": "SCH", "tables": list(tables)}


def _mock_session(cortex_response="", sql_rows=None):
    session = MagicMock()
    if sql_rows is None:
        sql_rows = []
    cortex_result = [[cortex_response]]
    sql_result = sql_rows

    def sql_side_effect(query):
        mock_result = MagicMock()
        if "CORTEX.COMPLETE" in query:
            mock_result.collect.return_value = cortex_result
        else:
            mock_result.collect.return_value = sql_result
        return mock_result

    session.sql.side_effect = sql_side_effect
    return session


class TestColSummary:
    def test_contains_name_and_type(self):
        from weaver.scenarios import _col_summary
        col = _col("ISU_CD", "VARCHAR")
        result = _col_summary(col)
        assert "ISU_CD" in result
        assert "VARCHAR" in result

    def test_includes_sample_values(self):
        from weaver.scenarios import _col_summary
        col = _col("MKT", "VARCHAR", samples=["STK", "KSQ"])
        assert "STK" in _col_summary(col)

    def test_includes_comment(self):
        from weaver.scenarios import _col_summary
        col = _col("TRD_DT", "DATE", comment="trade date")
        assert "trade date" in _col_summary(col)

    def test_no_sample_or_comment_stays_clean(self):
        from weaver.scenarios import _col_summary
        col = _col("X", "NUMBER")
        result = _col_summary(col)
        assert "X" in result
        assert "NUMBER" in result


class TestBuildPrompt:
    def test_contains_table_name(self):
        from weaver.scenarios import _build_prompt
        t = _table("TRADES")
        assert "TRADES" in _build_prompt(t, "DB", "SCH", [])

    def test_contains_database_and_schema(self):
        from weaver.scenarios import _build_prompt
        t = _table("T")
        prompt = _build_prompt(t, "MYDB", "MYSCH", [])
        assert "MYDB" in prompt
        assert "MYSCH" in prompt

    def test_contains_column_names(self):
        from weaver.scenarios import _build_prompt
        t = _table("T", columns=[_col("ISU_CD", "VARCHAR"), _col("TRD_AMT", "NUMBER")])
        prompt = _build_prompt(t, "DB", "SCH", [])
        assert "ISU_CD" in prompt
        assert "TRD_AMT" in prompt

    def test_mentions_related_tables(self):
        from weaver.scenarios import _build_prompt
        t = _table("T1", columns=[_col("ISU_CD", "VARCHAR")])
        related = [_table("T2", columns=[_col("ISU_CD", "VARCHAR")])]
        assert "T2" in _build_prompt(t, "DB", "SCH", related)

    def test_json_schema_hint_present(self):
        from weaver.scenarios import _build_prompt
        t = _table("T")
        prompt = _build_prompt(t, "DB", "SCH", [])
        assert "scenarios" in prompt
        assert "question" in prompt


class TestParseScenarios:
    def test_extracts_scenarios_list(self):
        from weaver.scenarios import _parse_scenarios
        raw = json.dumps({"scenarios": [{"question": "What?", "sql": "SELECT 1"}]})
        result = _parse_scenarios(raw)
        assert len(result) == 1
        assert result[0]["question"] == "What?"

    def test_filters_incomplete_scenarios(self):
        from weaver.scenarios import _parse_scenarios
        raw = json.dumps({"scenarios": [
            {"question": "Q", "sql": "SELECT 1"},
            {"question": "", "sql": "SELECT 2"},
            {"question": "Q2"},
        ]})
        result = _parse_scenarios(raw)
        assert len(result) == 1

    def test_raises_on_no_json(self):
        from weaver.scenarios import _parse_scenarios
        with pytest.raises(ValueError):
            _parse_scenarios("no json here")

    def test_embedded_json_in_prose(self):
        from weaver.scenarios import _parse_scenarios
        raw = 'Here:\n{"scenarios": [{"question": "Q", "sql": "SELECT 1"}]}\nDone.'
        result = _parse_scenarios(raw)
        assert len(result) == 1


class TestExecuteSql:
    def test_returns_string_on_success(self):
        from weaver.scenarios import _execute_sql
        row = MagicMock()
        row.__fields__ = ["A", "B"]
        row.__iter__ = lambda s: iter([1, 2])
        session = MagicMock()
        session.sql.return_value.collect.return_value = [row]
        result = _execute_sql(session, "SELECT 1")
        assert result is not None
        assert isinstance(result, str)

    def test_returns_no_results_string_for_empty(self):
        from weaver.scenarios import _execute_sql
        session = MagicMock()
        session.sql.return_value.collect.return_value = []
        assert _execute_sql(session, "SELECT 1") == "No results."

    def test_returns_none_on_exception(self):
        from weaver.scenarios import _execute_sql
        session = MagicMock()
        session.sql.side_effect = RuntimeError("bad sql")
        assert _execute_sql(session, "SELECT 1") is None


class TestFindRelated:
    def test_finds_tables_with_shared_column_names(self):
        from weaver.scenarios import _find_related
        t1 = _table("T1", columns=[_col("ISU_CD", "VARCHAR")])
        t2 = _table("T2", columns=[_col("ISU_CD", "VARCHAR")])
        t3 = _table("T3", columns=[_col("OTHER", "VARCHAR")])
        related = _find_related(t1, [t1, t2, t3])
        assert any(r["name"] == "T2" for r in related)
        assert not any(r["name"] == "T3" for r in related)

    def test_excludes_self(self):
        from weaver.scenarios import _find_related
        t = _table("T", columns=[_col("A", "VARCHAR")])
        related = _find_related(t, [t])
        assert related == []


class TestScenarioGeneratorGenerate:
    def _good_response(self, question="How many trades?", sql="SELECT COUNT(*) FROM DB.SCH.T"):
        return json.dumps({
            "scenarios": [{"question": question, "sql": sql}]
        })

    def test_returns_tuple_of_lists(self):
        from weaver.scenarios import ScenarioGenerator
        session = _mock_session(cortex_response=self._good_response())
        profile = _profile(_table("T", columns=[_col("A", "VARCHAR")]))
        result = ScenarioGenerator(session).generate(profile)
        assert isinstance(result, tuple)
        golden_set, questions = result
        assert isinstance(golden_set, list)
        assert isinstance(questions, list)

    def test_golden_set_has_query_and_expected_response(self):
        from weaver.scenarios import ScenarioGenerator
        session = _mock_session(cortex_response=self._good_response())
        profile = _profile(_table("T", columns=[_col("A", "VARCHAR")]))
        golden_set, _ = ScenarioGenerator(session).generate(profile)
        if golden_set:
            assert "query" in golden_set[0]
            assert "expected_response" in golden_set[0]

    def test_questions_match_golden_set(self):
        from weaver.scenarios import ScenarioGenerator
        session = _mock_session(cortex_response=self._good_response())
        profile = _profile(_table("T", columns=[_col("A", "VARCHAR")]))
        golden_set, questions = ScenarioGenerator(session).generate(profile)
        assert len(golden_set) == len(questions)
        if questions:
            assert questions[0] == golden_set[0]["query"]

    def test_cortex_failure_skips_table(self):
        from weaver.scenarios import ScenarioGenerator
        session = MagicMock()
        session.sql.side_effect = RuntimeError("Cortex down")
        profile = _profile(_table("T", columns=[_col("A", "VARCHAR")]))
        golden_set, questions = ScenarioGenerator(session).generate(profile)
        assert golden_set == []
        assert questions == []

    def test_sql_failure_drops_scenario(self):
        from weaver.scenarios import ScenarioGenerator
        session = MagicMock()

        def sql_side_effect(query):
            mock = MagicMock()
            if "CORTEX.COMPLETE" in query:
                mock.collect.return_value = [[self._good_response()]]
            else:
                mock.collect.side_effect = RuntimeError("bad SQL")
            return mock

        session.sql.side_effect = sql_side_effect
        profile = _profile(_table("T", columns=[_col("A", "VARCHAR")]))
        golden_set, questions = ScenarioGenerator(session).generate(profile)
        assert golden_set == []

    def test_one_cortex_call_per_table(self):
        from weaver.scenarios import ScenarioGenerator
        session = _mock_session(cortex_response=self._good_response())
        profile = _profile(
            _table("T1", columns=[_col("A", "VARCHAR")]),
            _table("T2", columns=[_col("B", "DATE")]),
        )
        ScenarioGenerator(session).generate(profile)
        cortex_calls = [
            c for c in session.sql.call_args_list
            if "CORTEX.COMPLETE" in str(c)
        ]
        assert len(cortex_calls) == 2
