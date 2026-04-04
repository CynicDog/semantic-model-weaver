"""
Unit tests for forge.evaluator — no Snowflake connection required.

All TruLens and Snowflake I/O is mocked via pytest-mock. What is tested:

  CortexAnalystApp  — ask() delegation and answer extraction
  build_session     — SnowflakeConnector + TruSession wiring
  build_feedbacks   — Cortex provider init, feedback count, feedback names
  build_tru_app     — TruApp construction with correct app_name / version
  run_evaluation    — one TruApp context per question, correct ask() calls
  get_results       — delegates to tru_session.get_records_and_feedback
"""

from unittest.mock import MagicMock, call

import pytest


class TestCortexAnalystApp:
    def test_ask_returns_answer_from_probe(self):
        from forge.evaluator import CortexAnalystApp

        probe = MagicMock()
        probe.query.return_value = {"answer": "삼성전자", "sql": "SELECT ...", "success": True}

        app = CortexAnalystApp(probe)
        result = app.ask("가장 많이 거래된 종목은?")

        probe.query.assert_called_once_with("가장 많이 거래된 종목은?")
        assert result == "삼성전자"

    def test_ask_returns_empty_string_when_answer_key_missing(self):
        from forge.evaluator import CortexAnalystApp

        probe = MagicMock()
        probe.query.return_value = {"sql": None, "success": False}

        app = CortexAnalystApp(probe)
        assert app.ask("unanswerable question") == ""

    def test_ask_returns_empty_string_when_answer_is_empty(self):
        from forge.evaluator import CortexAnalystApp

        probe = MagicMock()
        probe.query.return_value = {"answer": "", "success": True}

        app = CortexAnalystApp(probe)
        assert app.ask("question") == ""

    def test_ask_passes_question_to_probe_unchanged(self):
        from forge.evaluator import CortexAnalystApp

        probe = MagicMock()
        probe.query.return_value = {"answer": "OK"}

        app = CortexAnalystApp(probe)
        question = "월별 평균 거래대금은?"
        app.ask(question)

        probe.query.assert_called_once_with(question)

    def test_probe_protocol_satisfied_by_any_class_with_query(self):
        """Any object with a .query() method satisfies the Probe Protocol."""
        from forge.evaluator import Probe

        class FakeProbe:
            def query(self, question: str) -> dict:
                return {}

        assert isinstance(FakeProbe(), Probe)


class TestBuildSession:
    def test_creates_snowflake_connector_with_session(self, mocker):
        mock_connector_cls = mocker.patch("forge.evaluator.SnowflakeConnector")
        mocker.patch("forge.evaluator.TruSession")
        snowpark_session = MagicMock()

        from forge.evaluator import build_session

        build_session(snowpark_session)

        mock_connector_cls.assert_called_once_with(snowpark_session=snowpark_session)

    def test_creates_tru_session_with_connector(self, mocker):
        mock_connector_cls = mocker.patch("forge.evaluator.SnowflakeConnector")
        mock_session_cls = mocker.patch("forge.evaluator.TruSession")
        snowpark_session = MagicMock()

        from forge.evaluator import build_session

        build_session(snowpark_session)

        mock_session_cls.assert_called_once_with(
            connector=mock_connector_cls.return_value
        )

    def test_returns_tru_session(self, mocker):
        mocker.patch("forge.evaluator.SnowflakeConnector")
        mock_session_cls = mocker.patch("forge.evaluator.TruSession")
        snowpark_session = MagicMock()

        from forge.evaluator import build_session

        result = build_session(snowpark_session)

        assert result is mock_session_cls.return_value


class TestBuildFeedbacks:
    @pytest.fixture(autouse=True)
    def _mock_trulens(self, mocker):
        self.mock_cortex = mocker.patch("forge.evaluator.Cortex")
        self.mock_gt_cls = mocker.patch("forge.evaluator.GroundTruthAgreement")
        self.mock_feedback_cls = mocker.patch("forge.evaluator.Feedback")

        mock_fb = MagicMock()
        mock_fb.on_input_output.return_value = mock_fb
        self.mock_feedback_cls.return_value = mock_fb
        self.mock_fb = mock_fb

    def test_returns_two_feedbacks(self):
        from forge.evaluator import build_feedbacks

        feedbacks = build_feedbacks(MagicMock(), [{"query": "q", "expected_response": "a"}])

        assert len(feedbacks) == 2

    def test_uses_cortex_provider_with_correct_model(self):
        from forge.evaluator import build_feedbacks

        snowpark_session = MagicMock()
        build_feedbacks(snowpark_session, [])

        self.mock_cortex.assert_called_once_with(
            snowpark_session=snowpark_session,
            model_engine="llama3.1-8b",
        )

    def test_feedback_names_include_answer_relevance(self):
        from forge.evaluator import build_feedbacks

        build_feedbacks(MagicMock(), [{"query": "q", "expected_response": "a"}])

        names = [c.kwargs["name"] for c in self.mock_feedback_cls.call_args_list]
        assert "answer_relevance" in names

    def test_feedback_names_include_answer_correctness(self):
        from forge.evaluator import build_feedbacks

        build_feedbacks(MagicMock(), [{"query": "q", "expected_response": "a"}])

        names = [c.kwargs["name"] for c in self.mock_feedback_cls.call_args_list]
        assert "answer_correctness" in names

    def test_ground_truth_agreement_receives_golden_set(self):
        from forge.evaluator import build_feedbacks

        golden_set = [
            {"query": "종목 수는?", "expected_response": "900개"},
            {"query": "시장은?", "expected_response": "KOSPI"},
        ]
        build_feedbacks(MagicMock(), golden_set)

        self.mock_gt_cls.assert_called_once()
        args, _ = self.mock_gt_cls.call_args
        assert args[0] == golden_set


class TestBuildTruApp:
    def test_creates_tru_app_with_correct_app_name(self, mocker):
        mock_tru_app_cls = mocker.patch("forge.evaluator.TruApp")
        from forge.evaluator import APP_NAME, build_tru_app, CortexAnalystApp

        app = CortexAnalystApp(MagicMock())
        build_tru_app(app, feedbacks=[], version="v1")

        _, kwargs = mock_tru_app_cls.call_args
        assert kwargs["app_name"] == APP_NAME

    def test_creates_tru_app_with_given_version(self, mocker):
        mock_tru_app_cls = mocker.patch("forge.evaluator.TruApp")
        from forge.evaluator import build_tru_app, CortexAnalystApp

        app = CortexAnalystApp(MagicMock())
        build_tru_app(app, feedbacks=[], version="v2.iter3")

        _, kwargs = mock_tru_app_cls.call_args
        assert kwargs["app_version"] == "v2.iter3"

    def test_passes_feedbacks_to_tru_app(self, mocker):
        mock_tru_app_cls = mocker.patch("forge.evaluator.TruApp")
        from forge.evaluator import build_tru_app, CortexAnalystApp

        app = CortexAnalystApp(MagicMock())
        feedbacks = [MagicMock(), MagicMock()]
        build_tru_app(app, feedbacks=feedbacks)

        _, kwargs = mock_tru_app_cls.call_args
        assert kwargs["feedbacks"] is feedbacks


class TestRunEvaluation:
    def _make_tru_app(self):
        tru_app = MagicMock()
        tru_app.__enter__ = MagicMock(return_value=tru_app)
        tru_app.__exit__ = MagicMock(return_value=False)
        return tru_app

    def test_opens_one_context_per_question(self):
        from forge.evaluator import run_evaluation

        tru_app = self._make_tru_app()
        app = MagicMock()
        questions = ["q1", "q2", "q3"]

        run_evaluation(tru_app, app, questions)

        assert tru_app.__enter__.call_count == 3

    def test_calls_ask_once_per_question(self):
        from forge.evaluator import run_evaluation

        tru_app = self._make_tru_app()
        app = MagicMock()
        questions = ["q1", "q2", "q3"]

        run_evaluation(tru_app, app, questions)

        assert app.ask.call_count == 3

    def test_passes_each_question_to_ask(self):
        from forge.evaluator import run_evaluation

        tru_app = self._make_tru_app()
        app = MagicMock()
        questions = ["q1", "q2", "q3"]

        run_evaluation(tru_app, app, questions)

        app.ask.assert_has_calls([call("q1"), call("q2"), call("q3")])

    def test_empty_questions_list_is_no_op(self):
        from forge.evaluator import run_evaluation

        tru_app = self._make_tru_app()
        app = MagicMock()

        run_evaluation(tru_app, app, [])

        app.ask.assert_not_called()
        tru_app.__enter__.assert_not_called()


class TestGetResults:
    def test_delegates_to_tru_session(self):
        from forge.evaluator import APP_NAME, get_results

        tru_session = MagicMock()
        get_results(tru_session)

        tru_session.get_records_and_feedback.assert_called_once_with(app_name=APP_NAME)

    def test_returns_session_result_directly(self):
        from forge.evaluator import get_results

        tru_session = MagicMock()
        expected = (MagicMock(), MagicMock())
        tru_session.get_records_and_feedback.return_value = expected

        result = get_results(tru_session)

        assert result is expected
