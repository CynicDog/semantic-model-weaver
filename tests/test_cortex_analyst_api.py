"""
Integration tests — Cortex Analyst REST API validation.

These tests POST a generated YAML to the real Cortex Analyst endpoint and
assert that the API accepts the model (returns SQL, not a 400 parse error).
This is the ground truth: Snowflake does not publish a JSON Schema for the
semantic model format, so the API response IS the schema validator.

Run with:
    uv run pytest tests/test_cortex_analyst_api.py -v -m integration

Skipped automatically when SNOWFLAKE_PASSWORD is not set in the environment.
"""

import json
import logging
import os
from pathlib import Path

import pytest
import requests
import snowflake.connector
from dotenv import load_dotenv

from forge.dsl import (
    BaseTable,
    DataType,
    Dimension,
    Measure,
    SemanticModel,
    SemanticTable,
    TimeDimension,
)

load_dotenv()

log = logging.getLogger(__name__)

pytestmark = pytest.mark.integration


def _analyst_url() -> str:
    account = os.environ["FORGE_SNOWFLAKE_ACCOUNT"]
    return f"https://{account}.snowflakecomputing.com/api/v2/cortex/analyst/message"


def _log_response(resp: requests.Response) -> None:
    log.info("status=%d body=%s", resp.status_code, json.dumps(resp.json(), ensure_ascii=False))


def _analyst_request(yaml_text: str, question: str, token: str) -> requests.Response:
    return requests.post(
        _analyst_url(),
        headers={
            "Authorization": f'Snowflake Token="{token}"',
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
        json={
            "messages": [{"role": "user", "content": [{"type": "text", "text": question}]}],
            "semantic_model": yaml_text,
        },
        timeout=30,
    )


@pytest.fixture(scope="module")
def snowflake_token():
    """
    Yield a live session token for the duration of the module.
    The connection must stay open — closing it invalidates the token.
    """
    account = os.environ.get("FORGE_SNOWFLAKE_ACCOUNT")
    user = os.environ.get("FORGE_SNOWFLAKE_USER")
    pwd = os.environ.get("FORGE_SNOWFLAKE_PASSWORD")
    role = os.environ.get("FORGE_SNOWFLAKE_ROLE", "ACCOUNTADMIN")
    if not all([account, user, pwd]):
        pytest.skip("FORGE_SNOWFLAKE_* env vars not set — skipping integration tests")
    conn = snowflake.connector.connect(
        account=account,
        user=user,
        password=pwd,
        role=role,
    )
    try:
        yield conn._rest.token
    finally:
        conn.close()


@pytest.fixture(scope="module")
def minimal_analyst_model():
    """Minimal single-table model over NEXTRADE_EQUITY_MARKET_DATA.FIN.NX_HT_BAT_REFER_A0."""
    return SemanticModel(
        name="forge_integration_test",
        description="Minimal model for API acceptance testing",
        tables=[
            SemanticTable(
                name="STOCK_REFERENCE",
                description="Stock master reference — one row per stock per date.",
                base_table=BaseTable(
                    **{
                        "database": "NEXTRADE_EQUITY_MARKET_DATA",
                        "schema": "FIN",
                        "table": "NX_HT_BAT_REFER_A0",
                    }
                ),
                time_dimensions=[
                    TimeDimension(
                        name="DW_DATE",
                        expr="DWDD",
                        data_type=DataType.DATE,
                        description="Batch date.",
                        synonyms=["date", "날짜"],
                    )
                ],
                dimensions=[
                    Dimension(
                        name="STOCK_CODE",
                        expr="ISU_CD",
                        data_type=DataType.VARCHAR,
                        description="Full 12-character issue code.",
                        synonyms=["ticker", "종목코드"],
                    ),
                    Dimension(
                        name="MARKET_ID",
                        expr="MKT_ID",
                        data_type=DataType.VARCHAR,
                        description="Market. STK=KOSPI, KSQ=KOSDAQ.",
                        synonyms=["market", "시장"],
                        sample_values=["STK", "KSQ"],
                    ),
                ],
                measures=[
                    Measure(
                        name="LISTED_SHARES",
                        expr="SUM(LSTG_STCNT)",
                        data_type=DataType.NUMBER,
                        description="Total listed share count.",
                        synonyms=["shares", "상장주식수"],
                    )
                ],
            )
        ],
    )


def test_api_accepts_minimal_model(minimal_analyst_model, snowflake_token):
    """
    The API must return 200 for a structurally valid model.
    A response without an 'error' content type means Cortex Analyst parsed the YAML.
    """
    resp = _analyst_request(
        yaml_text=minimal_analyst_model.to_yaml(),
        question="How many stocks are listed on KOSPI?",
        token=snowflake_token,
    )
    _log_response(resp)
    assert resp.status_code == 200, (
        f"Expected 200, got {resp.status_code}.\nResponse: {resp.text[:500]}"
    )
    body = resp.json()
    assert "message" in body
    content_types = [c.get("type") for c in body["message"].get("content", [])]
    assert "error" not in content_types, (
        f"Cortex Analyst returned an error — YAML likely malformed.\n"
        f"Content: {body['message']['content']}"
    )


def test_api_returns_sql_for_count_question(snowflake_token):
    """
    A simple count question against a dimension-only model should yield an sql response.
    COUNT DISTINCT on a dimension is unambiguous and reliably generates SQL.
    """
    model = SemanticModel(
        name="count_test",
        tables=[
            SemanticTable(
                name="STOCK_REFERENCE",
                base_table=BaseTable(
                    **{
                        "database": "NEXTRADE_EQUITY_MARKET_DATA",
                        "schema": "FIN",
                        "table": "NX_HT_BAT_REFER_A0",
                    }  # noqa: E501
                ),
                dimensions=[
                    Dimension(
                        name="STOCK_CODE",
                        expr="ISU_CD",
                        data_type=DataType.VARCHAR,
                        synonyms=["ticker"],
                    ),  # noqa: E501
                    Dimension(
                        name="MARKET_ID",
                        expr="MKT_ID",
                        data_type=DataType.VARCHAR,
                        synonyms=["market"],
                    ),  # noqa: E501
                ],
            )
        ],
    )
    resp = _analyst_request(
        yaml_text=model.to_yaml(),
        question="How many distinct stocks are there?",
        token=snowflake_token,
    )
    _log_response(resp)
    assert resp.status_code == 200
    content = resp.json()["message"]["content"]
    types = {c["type"] for c in content}
    assert "sql" in types, f"Expected sql for a simple count question.\nContent: {content}"


def test_api_rejects_invalid_yaml(snowflake_token):
    """
    Deliberately malformed YAML must trigger an error response.
    This ensures the acceptance tests above are meaningful — the API does validate.
    """
    broken_yaml = "name: broken\ntables:\n  - name: T\n    base_table: NOT_A_MAPPING\n"
    resp = _analyst_request(
        yaml_text=broken_yaml,
        question="anything",
        token=snowflake_token,
    )
    _log_response(resp)
    is_http_error = resp.status_code >= 400
    body = resp.json()
    message = body.get("message", {})
    content = message.get("content", []) if isinstance(message, dict) else []
    is_content_error = resp.status_code == 200 and any(c.get("type") == "error" for c in content)
    assert is_http_error or is_content_error, (
        f"Expected an error for malformed YAML, got {resp.status_code}.\n{resp.text[:300]}"
    )


def test_nti_model_yaml_accepted_by_api(snowflake_token):
    """
    nti_model.yaml must survive a forge.dsl round-trip and still be accepted by the API.
    Verifies the DSL does not corrupt a known-good model during serialisation.
    """
    nti_yaml_path = (
        Path(__file__).parent.parent / "examples/streamlit-on-snowflake/manifest/nti_model.yaml"
    )
    model = SemanticModel.from_yaml_file(str(nti_yaml_path))
    resp = _analyst_request(
        yaml_text=model.to_yaml(),
        question="What was the total trading volume on KOSPI last month?",
        token=snowflake_token,
    )
    _log_response(resp)
    assert resp.status_code == 200, (
        f"nti_model.yaml round-tripped through DSL was rejected by Cortex Analyst.\n"
        f"Status: {resp.status_code}\n{resp.text[:500]}"
    )
