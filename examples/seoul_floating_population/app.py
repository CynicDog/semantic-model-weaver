"""
Seoul Floating Population & Assets · Cortex Analyst Chat
---------------------------------------------------------
A minimal Streamlit chat app that talks to the Snowflake Cortex Analyst REST API
using the SPH district-level floating population, consumption, and assets semantic model.

Run:
    uv run streamlit run app.py
"""

import os
import re
from pathlib import Path

import requests
import streamlit as st
from dotenv import load_dotenv
from snowflake.snowpark import Session

for _env_path in [Path(".env"), Path("../../../.env"), Path("../../.env")]:
    if _env_path.exists():
        load_dotenv(_env_path)
        break

_MODEL_YAML = os.environ.get("WEAVER_MODEL_YAML", str(Path(__file__).parent / "model.yaml"))
_TIMEOUT = 90
_API_PATH = "/api/v2/cortex/analyst/message"


@st.cache_resource(show_spinner="Connecting to Snowflake…")
def _session() -> Session:
    return Session.builder.configs({
        "account":   os.environ["WEAVER_SNOWFLAKE_ACCOUNT"],
        "user":      os.environ["WEAVER_SNOWFLAKE_USER"],
        "password":  os.environ["WEAVER_SNOWFLAKE_PASSWORD"],
        "role":      os.environ.get("WEAVER_SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
        "warehouse": os.environ.get("WEAVER_SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        "database":  os.environ.get("WEAVER_SNOWFLAKE_DATABASE", "SEMANTIC_MODEL_WEAVER"),
        "schema":    os.environ.get("WEAVER_SNOWFLAKE_SCHEMA", "trulens"),
    }).create()


@st.cache_data(show_spinner=False)
def _yaml() -> str:
    return Path(_MODEL_YAML).read_text(encoding="utf-8")


def _token(session: Session) -> str:
    return session._conn._conn._rest._token


def _base_url(session: Session) -> str:
    account = session.get_current_account().strip('"').lower()
    return f"https://{account}.snowflakecomputing.com"


def _call_analyst(session: Session, api_messages: list) -> dict:
    resp = requests.post(
        _base_url(session) + _API_PATH,
        headers={
            "Authorization": f'Snowflake Token="{_token(session)}"',
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
        json={"messages": api_messages, "semantic_model": _yaml()},
        timeout=_TIMEOUT,
    )
    resp.raise_for_status()
    return resp.json()


def _parse(data: dict) -> tuple[str, str | None]:
    content = data.get("message", {}).get("content", [])
    text, sql = "", None
    for block in content:
        if not isinstance(block, dict):
            continue
        if block.get("type") == "text":
            text = block.get("text", "")
        elif block.get("type") == "sql":
            sql = block.get("statement")
    return text, sql


def _to_api_messages(history: list[dict]) -> list[dict]:
    return [
        {
            "role": msg["role"],
            "content": [{"type": "text", "text": msg["content"]}],
        }
        for msg in history
        if msg["role"] in ("user", "analyst")
    ]


def _run_sql(session: Session, sql: str):
    try:
        clean = re.sub(r"--[^\n]*", "", sql).strip().rstrip(";")
        return session.sql(clean).to_pandas()
    except Exception as exc:
        st.caption(f"⚠ Could not execute SQL: {exc}")
        return None


def _render_results(df) -> None:
    if df is None or df.empty:
        st.caption("No rows returned.")
        return

    st.dataframe(df, use_container_width=True)

    num_cols = df.select_dtypes("number").columns.tolist()
    date_cols = [c for c in df.columns if re.search(r"date|yyyymm|ymd|time|period", c, re.I)]
    cat_cols = [c for c in df.columns if df[c].dtype == object and c not in date_cols]

    if len(df) > 1 and num_cols:
        if date_cols:
            with st.expander("Chart", expanded=True):
                st.line_chart(df.set_index(date_cols[0])[num_cols[:3]])
        elif cat_cols and len(df) <= 50:
            with st.expander("Chart", expanded=True):
                st.bar_chart(df.set_index(cat_cols[0])[num_cols[:1]])


st.set_page_config(
    page_title="Seoul Floating Population · Cortex Analyst",
    page_icon="🗺️",
    layout="wide",
)
st.markdown("""
<style>
    [data-testid="stSidebar"] { min-width: 480px; max-width: 480px; }
</style>
""", unsafe_allow_html=True)

st.title("🗺️ SPH / GranData — 서울 생활인구·소비·자산 데이터 · Cortex Analyst")
st.caption(
    "SPH / GranData — 서울 생활인구·소비·자산 데이터 · Ask questions in plain English or Korean."
    " Powered by Snowflake Cortex Analyst."
)

with st.sidebar:
    st.header("Model")
    st.code(Path(_MODEL_YAML).name, language=None)
    with st.expander("Semantic YAML", expanded=False):
        st.code(_yaml(), language="yaml")
    if st.button("Clear conversation"):
        st.session_state.messages = []
        st.rerun()
    st.divider()
    st.caption("Connected as **ACCOUNTADMIN**")

session = _session()

if "messages" not in st.session_state:
    st.session_state.messages = []

for msg in st.session_state.messages:
    role_label = "assistant" if msg["role"] == "analyst" else msg["role"]
    with st.chat_message(role_label):
        st.markdown(msg["content"])
        if msg.get("sql"):
            with st.expander("Generated SQL"):
                st.code(msg["sql"], language="sql")
        if msg.get("results") is not None:
            _render_results(msg["results"])

_placeholder = (
    "e.g. 강남구 유동인구가 가장 많은 시간대는?"
    "  /  Which district has the highest average household assets?"
)
if prompt := st.chat_input(_placeholder):
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        with st.spinner("Thinking…"):
            try:
                data = _call_analyst(session, _to_api_messages(st.session_state.messages))
                answer, sql = _parse(data)

                st.markdown(answer or "_No answer returned._")
                results = None
                if sql:
                    with st.expander("Generated SQL"):
                        st.code(sql, language="sql")
                    results = _run_sql(session, sql)
                    _render_results(results)

                st.session_state.messages.append({
                    "role": "analyst",
                    "content": answer,
                    "sql": sql,
                    "results": results,
                })
            except requests.HTTPError as exc:
                err = exc.response.text[:400] if exc.response else str(exc)
                st.error(f"Cortex Analyst error {exc.response.status_code}: {err}")
            except Exception as exc:
                st.error(f"Error: {exc}")
