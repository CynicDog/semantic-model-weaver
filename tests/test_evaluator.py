"""
Unit tests for weaver.evaluator — no Snowflake connection required.

All TruLens and Snowflake I/O is mocked via pytest-mock. What is tested:

  CortexAnalystApp  — ask() delegation and answer extraction
  build_session     — SnowflakeConnector + TruSession wiring
  build_metrics     — Cortex provider init, metric count, metric names
  build_tru_app     — TruApp construction with correct app_name, version, main_method_name
  run_evaluation    — live_run context, per-question input(), ingestion wait, compute_metrics
  get_results       — delegates to tru_session.get_records_and_feedback
"""

import time
from unittest.mock import MagicMock, call, patch

import pytest

from trulens.core.run import RunStatus


class TestCortexAnalystApp:
    def test_ask_returns_answer_from_probe(self):
        from weaver.evaluator import CortexAnalystApp

        probe = MagicMock()
        probe.query.return_value = {"answer": "삼성전자", "sql": "SELECT ...", "success": True}

        app = CortexAnalystApp(probe)
        result = app.ask("가장 많이 거래된 종목은?")

        probe.query.assert_called_once_with("가장 많이 거래된 종목은?")
        assert result == "삼성전자"

    def test_ask_returns_empty_string_when_answer_key_missing(self):
        from weaver.evaluator import CortexAnalystApp

        probe = MagicMock()
        probe.query.return_value = {"sql": None, "success": False}

        app = CortexAnalystApp(probe)
        assert app.ask("unanswerable question") == ""

    def test_ask_returns_empty_string_when_answer_is_empty(self):
        from weaver.evaluator import CortexAnalystApp

        probe = MagicMock()
        probe.query.return_value = {"answer": "", "success": True}

        app = CortexAnalystApp(probe)
        assert app.ask("question") == ""

    def test_ask_passes_question_to_probe_unchanged(self):
        from weaver.evaluator import CortexAnalystApp

        probe = MagicMock()
        probe.query.return_value = {"answer": "OK"}

        app = CortexAnalystApp(probe)
        app.ask("월별 평균 거래대금은?")

        probe.query.assert_called_once_with("월별 평균 거래대금은?")

    def test_probe_protocol_satisfied_by_any_class_with_query(self):
        from weaver.evaluator import Probe

        class FakeProbe:
            def query(self, question: str) -> dict:
                return {}

        assert isinstance(FakeProbe(), Probe)


class TestBuildSession:
    def test_creates_snowflake_connector_with_session(self, mocker):
        mock_connector_cls = mocker.patch("weaver.evaluator.SnowflakeConnector")
        mocker.patch("weaver.evaluator.TruSession")
        snowpark_session = MagicMock()

        from weaver.evaluator import build_session

        build_session(snowpark_session)

        mock_connector_cls.assert_called_once_with(snowpark_session=snowpark_session)

    def test_creates_tru_session_with_connector(self, mocker):
        mock_connector_cls = mocker.patch("weaver.evaluator.SnowflakeConnector")
        mock_session_cls = mocker.patch("weaver.evaluator.TruSession")
        snowpark_session = MagicMock()

        from weaver.evaluator import build_session

        build_session(snowpark_session)

        mock_session_cls.assert_called_once_with(connector=mock_connector_cls.return_value)

    def test_returns_tru_session_and_connector(self, mocker):
        mock_connector_cls = mocker.patch("weaver.evaluator.SnowflakeConnector")
        mock_session_cls = mocker.patch("weaver.evaluator.TruSession")

        from weaver.evaluator import build_session

        tru_session, connector = build_session(MagicMock())

        assert tru_session is mock_session_cls.return_value
        assert connector is mock_connector_cls.return_value


class TestBuildMetrics:
    def test_returns_list_of_strings(self):
        from weaver.evaluator import build_metrics

        result = build_metrics(MagicMock(), [])

        assert isinstance(result, list)
        assert all(isinstance(m, str) for m in result)

    def test_returns_two_metrics(self):
        from weaver.evaluator import build_metrics

        result = build_metrics(MagicMock(), [])

        assert len(result) == 2

    def test_includes_answer_relevance(self):
        from weaver.evaluator import build_metrics

        result = build_metrics(MagicMock(), [])

        assert "answer_relevance" in result

    def test_includes_correctness(self):
        from weaver.evaluator import build_metrics

        result = build_metrics(MagicMock(), [])

        assert "correctness" in result


class TestBuildTruApp:
    def test_creates_tru_app_with_correct_app_name(self, mocker):
        mock_tru_app_cls = mocker.patch("weaver.evaluator.TruApp")
        from weaver.evaluator import APP_NAME, CortexAnalystApp, build_tru_app

        build_tru_app(CortexAnalystApp(MagicMock()), connector=MagicMock())

        _, kwargs = mock_tru_app_cls.call_args
        assert kwargs["app_name"] == APP_NAME

    def test_creates_tru_app_with_given_version(self, mocker):
        mock_tru_app_cls = mocker.patch("weaver.evaluator.TruApp")
        from weaver.evaluator import CortexAnalystApp, build_tru_app

        build_tru_app(CortexAnalystApp(MagicMock()), connector=MagicMock(), version="v2.iter3")

        _, kwargs = mock_tru_app_cls.call_args
        assert kwargs["app_version"] == "v2.iter3"

    def test_sets_main_method_name_to_ask(self, mocker):
        mock_tru_app_cls = mocker.patch("weaver.evaluator.TruApp")
        from weaver.evaluator import CortexAnalystApp, build_tru_app

        build_tru_app(CortexAnalystApp(MagicMock()), connector=MagicMock())

        _, kwargs = mock_tru_app_cls.call_args
        assert kwargs["main_method_name"] == "ask"

    def test_passes_connector_to_tru_app(self, mocker):
        mock_tru_app_cls = mocker.patch("weaver.evaluator.TruApp")
        from weaver.evaluator import CortexAnalystApp, build_tru_app

        connector = MagicMock()
        build_tru_app(CortexAnalystApp(MagicMock()), connector=connector)

        _, kwargs = mock_tru_app_cls.call_args
        assert kwargs["connector"] is connector

    def test_does_not_pass_feedbacks(self, mocker):
        mock_tru_app_cls = mocker.patch("weaver.evaluator.TruApp")
        from weaver.evaluator import CortexAnalystApp, build_tru_app

        build_tru_app(CortexAnalystApp(MagicMock()), connector=MagicMock())

        _, kwargs = mock_tru_app_cls.call_args
        assert "feedbacks" not in kwargs


class TestRunEvaluation:
    def _make_tru_app(self, run_status=RunStatus.INVOCATION_COMPLETED):
        tru_app = MagicMock()

        live_run_ctx = MagicMock()
        live_run_ctx.run.get_status.return_value = run_status

        input_ctx = MagicMock()
        input_ctx.__enter__ = MagicMock(return_value=None)
        input_ctx.__exit__ = MagicMock(return_value=False)
        live_run_ctx.input.return_value = input_ctx

        live_run_mgr = MagicMock()
        live_run_mgr.__enter__ = MagicMock(return_value=live_run_ctx)
        live_run_mgr.__exit__ = MagicMock(return_value=False)
        tru_app.live_run.return_value = live_run_mgr

        return tru_app, live_run_ctx

    def test_empty_questions_is_no_op(self):
        from weaver.evaluator import run_evaluation

        tru_app, live_run_ctx = self._make_tru_app()
        app = MagicMock()

        run_evaluation(tru_app, app, questions=[], metrics=[], version="v1")

        tru_app.live_run.assert_not_called()
        app.ask.assert_not_called()

    def test_opens_live_run_with_version_as_run_name(self):
        from weaver.evaluator import run_evaluation

        tru_app, _ = self._make_tru_app()
        app = MagicMock()

        run_evaluation(tru_app, app, questions=["q1"], metrics=[], version="v1.iter2")

        tru_app.live_run.assert_called_once_with(run_name="v1.iter2")

    def test_calls_ask_once_per_question(self):
        from weaver.evaluator import run_evaluation

        tru_app, _ = self._make_tru_app()
        app = MagicMock()

        run_evaluation(tru_app, app, ["q1", "q2", "q3"], metrics=[], version="v1")

        assert app.ask.call_count == 3

    def test_passes_each_question_to_ask(self):
        from weaver.evaluator import run_evaluation

        tru_app, _ = self._make_tru_app()
        app = MagicMock()

        run_evaluation(tru_app, app, ["q1", "q2", "q3"], metrics=[], version="v1")

        app.ask.assert_has_calls([call("q1"), call("q2"), call("q3")])

    def test_opens_input_context_per_question(self):
        from weaver.evaluator import run_evaluation

        tru_app, live_run_ctx = self._make_tru_app()
        app = MagicMock()

        run_evaluation(tru_app, app, ["q1", "q2", "q3"], metrics=[], version="v1")

        assert live_run_ctx.input.call_count == 3

    def test_calls_compute_metrics_after_ingestion(self):
        from weaver.evaluator import run_evaluation

        tru_app, live_run_ctx = self._make_tru_app(RunStatus.INVOCATION_COMPLETED)
        app = MagicMock()
        metrics = ["answer_relevance", "correctness"]

        run_evaluation(tru_app, app, ["q1"], metrics=metrics, version="v1")

        live_run_ctx.run.compute_metrics.assert_called_once_with(metrics=metrics)

    def test_skips_compute_metrics_on_failed_run(self, caplog):
        import logging
        from weaver.evaluator import run_evaluation

        tru_app, live_run_ctx = self._make_tru_app(RunStatus.FAILED)
        app = MagicMock()

        with caplog.at_level(logging.WARNING, logger="weaver.evaluator"):
            run_evaluation(tru_app, app, ["q1"], metrics=["answer_relevance"], version="v1")

        live_run_ctx.run.compute_metrics.assert_not_called()
        assert any("failed during ingestion" in r.message for r in caplog.records)

    def test_polls_until_ingestion_completes(self, mocker):
        from weaver.evaluator import run_evaluation

        tru_app, live_run_ctx = self._make_tru_app()
        app = MagicMock()
        mock_sleep = mocker.patch("weaver.evaluator.time.sleep")

        statuses = [RunStatus.INVOCATION_IN_PROGRESS, RunStatus.INVOCATION_IN_PROGRESS, RunStatus.INVOCATION_COMPLETED]
        live_run_ctx.run.get_status.side_effect = statuses

        run_evaluation(tru_app, app, ["q1"], metrics=[], version="v1")

        assert mock_sleep.call_count == 2


class TestGetResults:
    def test_delegates_to_tru_session(self):
        from weaver.evaluator import APP_NAME, get_results

        tru_session = MagicMock()
        get_results(tru_session)

        tru_session.get_records_and_feedback.assert_called_once_with(app_name=APP_NAME)

    def test_returns_session_result_directly(self):
        from weaver.evaluator import get_results

        tru_session = MagicMock()
        expected = (MagicMock(), MagicMock())
        tru_session.get_records_and_feedback.return_value = expected

        assert get_results(tru_session) is expected
