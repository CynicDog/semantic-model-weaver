"""
weaver.probe — CortexAnalystProbe

Fires natural-language questions at the Cortex Analyst REST API using a
draft semantic model YAML. Returns structured results for TruLens logging.

The probe is stateless — it holds the YAML string and fires one question
per call. The YAML can be swapped between refinement iterations by creating
a new probe instance.

Satisfies the Probe protocol defined in weaver/evaluator.py:
    def query(self, question: str) -> dict:
        # returns: {"answer": str, "sql": str | None, "success": bool}

Auth: Snowpark session token, extracted from the active session connection.
This avoids requiring a separate key-pair setup and works with the same
password-auth session used everywhere else in the pipeline.

Usage:
    probe = CortexAnalystProbe(session, semantic_model.to_yaml())
    result = probe.query("이번 달 종목별 거래량 상위 5개는?")
    # result = {"answer": "...", "sql": "SELECT ...", "success": True}
"""

from __future__ import annotations

import json
import logging

import requests

from snowflake.snowpark import Session

log = logging.getLogger(__name__)

_API_PATH = "/api/v2/cortex/analyst/message"
_TIMEOUT = 120


def _extract_token(session: Session) -> str:
    conn = session._conn._conn
    return conn._rest._token


def _account_url(session: Session) -> str:
    account = session.get_current_account().strip('"').lower()
    return f"https://{account}.snowflakecomputing.com"


def _parse_analyst_response(data: dict) -> dict:
    message = data.get("message", {})
    if not isinstance(message, dict):
        return {"answer": "", "sql": None, "success": False}
    content = message.get("content", [])
    answer = ""
    sql = None
    for block in content:
        if not isinstance(block, dict):
            continue
        if block.get("type") == "text":
            answer = block.get("text", "")
        elif block.get("type") == "sql":
            sql = block.get("statement")
    return {"answer": answer, "sql": sql, "success": True}


class CortexAnalystProbe:
    """
    Fires NL questions at Cortex Analyst REST API with a given semantic YAML.

    Each probe instance is bound to one YAML snapshot. Create a new instance
    after each refinement iteration.

    Returns per-question:
        answer  (str)       — answer text from Cortex Analyst, or "" on failure
        sql     (str|None)  — generated SQL, or None on failure
        success (bool)      — False when the API call or execution failed
    """

    def __init__(self, session: Session, yaml_text: str) -> None:
        self._yaml = yaml_text
        self._session = session
        self._base_url = _account_url(session)

    def query(self, question: str) -> dict:
        url = self._base_url + _API_PATH
        headers = {
            "Authorization": f'Snowflake Token="{_extract_token(self._session)}"',
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        body = {
            "messages": [
                {
                    "role": "user",
                    "content": [{"type": "text", "text": question}],
                }
            ],
            "semantic_model": self._yaml,
        }

        try:
            resp = requests.post(url, headers=headers, json=body, timeout=_TIMEOUT)
            resp.raise_for_status()
            return _parse_analyst_response(resp.json())
        except requests.HTTPError as exc:
            log.warning(
                "CortexAnalystProbe: HTTP %s — %s | question: %s",
                exc.response.status_code, exc.response.text[:300], question,
            )
            return {"answer": "", "sql": None, "success": False}
        except Exception as exc:
            log.warning("CortexAnalystProbe: request failed — %s", exc)
            return {"answer": "", "sql": None, "success": False}
