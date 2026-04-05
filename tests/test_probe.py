"""
Unit tests for weaver.probe — no live Snowflake connection required.

  _parse_analyst_response   — extracts answer and sql from API response
  CortexAnalystProbe.query  — HTTP success, HTTP error, network failure
"""

import json
from unittest.mock import MagicMock, patch

import pytest
import requests


def _mock_session(account='"MY_ACCOUNT"', token="tok123"):
    session = MagicMock()
    session.get_current_account.return_value = account
    session._conn._conn._rest._token = token
    return session


def _analyst_response(answer="Five trades.", sql="SELECT COUNT(*) FROM T"):
    return {
        "message": {
            "role": "analyst",
            "content": [
                {"type": "text", "text": answer},
                {"type": "sql", "statement": sql},
            ],
        }
    }


class TestParseAnalystResponse:
    def test_extracts_answer_text(self):
        from weaver.probe import _parse_analyst_response
        data = _analyst_response(answer="There were 5 trades.")
        result = _parse_analyst_response(data)
        assert result["answer"] == "There were 5 trades."

    def test_extracts_sql(self):
        from weaver.probe import _parse_analyst_response
        data = _analyst_response(sql="SELECT 1")
        result = _parse_analyst_response(data)
        assert result["sql"] == "SELECT 1"

    def test_success_is_true(self):
        from weaver.probe import _parse_analyst_response
        result = _parse_analyst_response(_analyst_response())
        assert result["success"] is True

    def test_missing_sql_block_returns_none(self):
        from weaver.probe import _parse_analyst_response
        data = {"message": {"role": "analyst", "content": [{"type": "text", "text": "answer"}]}}
        result = _parse_analyst_response(data)
        assert result["sql"] is None

    def test_empty_content_returns_defaults(self):
        from weaver.probe import _parse_analyst_response
        result = _parse_analyst_response({"message": {"content": []}})
        assert result["answer"] == ""
        assert result["sql"] is None


class TestCortexAnalystProbeQuery:
    def _make_probe(self, yaml_text="name: test\ntables: []"):
        from weaver.probe import CortexAnalystProbe
        session = _mock_session()
        return CortexAnalystProbe(session, yaml_text)

    def test_successful_query_returns_answer(self):
        probe = self._make_probe()
        mock_resp = MagicMock()
        mock_resp.json.return_value = _analyst_response("42 trades")
        mock_resp.raise_for_status = MagicMock()

        with patch("weaver.probe.requests.post", return_value=mock_resp):
            result = probe.query("How many trades?")

        assert result["answer"] == "42 trades"
        assert result["success"] is True

    def test_successful_query_returns_sql(self):
        probe = self._make_probe()
        mock_resp = MagicMock()
        mock_resp.json.return_value = _analyst_response(sql="SELECT COUNT(*) FROM T")
        mock_resp.raise_for_status = MagicMock()

        with patch("weaver.probe.requests.post", return_value=mock_resp):
            result = probe.query("How many trades?")

        assert result["sql"] == "SELECT COUNT(*) FROM T"

    def test_http_error_returns_failure(self):
        probe = self._make_probe()
        mock_resp = MagicMock()
        mock_resp.status_code = 401
        mock_resp.text = "Unauthorized"
        http_err = requests.HTTPError(response=mock_resp)
        mock_resp.raise_for_status.side_effect = http_err

        with patch("weaver.probe.requests.post", return_value=mock_resp):
            result = probe.query("question")

        assert result["success"] is False
        assert result["answer"] == ""
        assert result["sql"] is None

    def test_network_error_returns_failure(self):
        probe = self._make_probe()
        with patch("weaver.probe.requests.post", side_effect=ConnectionError("timeout")):
            result = probe.query("question")

        assert result["success"] is False

    def test_request_sends_yaml_in_body(self):
        probe = self._make_probe(yaml_text="my_yaml_content")
        mock_resp = MagicMock()
        mock_resp.json.return_value = _analyst_response()
        mock_resp.raise_for_status = MagicMock()

        with patch("weaver.probe.requests.post", return_value=mock_resp) as mock_post:
            probe.query("question")

        body = mock_post.call_args.kwargs["json"]
        assert body["semantic_model"] == "my_yaml_content"

    def test_request_sends_question_in_messages(self):
        probe = self._make_probe()
        mock_resp = MagicMock()
        mock_resp.json.return_value = _analyst_response()
        mock_resp.raise_for_status = MagicMock()

        with patch("weaver.probe.requests.post", return_value=mock_resp) as mock_post:
            probe.query("How many trades today?")

        body = mock_post.call_args.kwargs["json"]
        text = body["messages"][0]["content"][0]["text"]
        assert text == "How many trades today?"

    def test_request_uses_snowflake_token_auth(self):
        from weaver.probe import CortexAnalystProbe
        session = _mock_session(token="mytoken")
        probe = CortexAnalystProbe(session, "yaml")
        mock_resp = MagicMock()
        mock_resp.json.return_value = _analyst_response()
        mock_resp.raise_for_status = MagicMock()

        with patch("weaver.probe.requests.post", return_value=mock_resp) as mock_post:
            probe.query("q")

        headers = mock_post.call_args.kwargs["headers"]
        assert "mytoken" in headers["Authorization"]

    def test_account_url_strips_quotes(self):
        from weaver.probe import CortexAnalystProbe
        session = _mock_session(account='"ZZ-ACCOUNT"')
        probe = CortexAnalystProbe(session, "yaml")
        assert "zz-account" in probe._base_url
        assert '"' not in probe._base_url
