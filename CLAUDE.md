# CLAUDE.md

This file provides guidance to Claude Code when working in this repository.

## Project

**Semantic Model Weaver** — Snowflake Hackathon 2026 Seoul

An agentic pipeline that reads a raw Snowflake database, auto-generates a Cortex Analyst-compliant semantic YAML, tests it with synthesized natural-language scenarios, scores quality via TruLens, and iteratively refines the model — all without human authoring.

The hackathon's four Marketplace datasets are not the domain. They are the benchmark subjects the system runs against to prove it works.

## Why this wins on the judging criteria

- **Creativity**: Nobody builds tooling *for* Cortex Analyst itself. Semantic model authoring is manual, blind, and unverifiable today. This solves that.
- **Snowflake Expertise**: The entire pipeline is Snowflake-native — Snowpark (schema discovery), Cortex Arctic (generation), Cortex Analyst REST API (probing), TruLens with Snowflake backend (evaluation). The solution cannot exist off-platform.
- **AI Expertise**: Generative AI is creating and evaluating *artifacts*, not answering questions. The agentic loop (generate → test → score → refine) is genuine new value.

## Pipeline Architecture

```
[SchemaDiscovery]
    Snowpark — reads INFORMATION_SCHEMA, profiles columns,
    samples text/boolean values (up to 5 distinct), infers FK candidates
    via column name + type-family matching.
        ↓
[YAMLWriter]
    Rule-based (no LLM) — classifies columns into dimensions, measures, and
    time_dimensions by type and name-suffix heuristics. Infers relationships
    from FK candidates. Output is always structurally valid.
        ↓
[QueryHistoryMiner]
    Mines SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY (90-day lookback, ≤1000 queries)
    — extracts column aliases from historical SQL to build a business vocabulary
    map {table → {col → [alias phrases]}} that grounds synonym enrichment.
        ↓
[SynonymEnricher]
    Cortex COMPLETE() (mistral-large2) — one call per table, prompted with column
    names/types/comments + query-history aliases. Writes descriptions and synonyms
    only; no structural changes. Raw-value synonyms and type-echo descriptions are
    filtered out before applying.
        ↓
[ScenarioGenerator]
    Cortex COMPLETE() (mistral-large2) — generates 5 NL questions per table,
    including related-table joins. Each question comes with a ground-truth SQL
    executed directly against Snowflake (Snowpark). Failed SQL is dropped silently.
        ↓
[CortexAnalystProbe]
    Cortex Analyst REST API — fires each NL question with the current YAML.
    Executes returned SQL via Snowpark and formats result as the answer string.
    One probe instance per refinement iteration (YAML is swapped between iterations).
        ↓
[Evaluator + TruLens OTEL]
    TruLens live_run → OTEL spans → Snowflake event table.
    Server-side metrics (answer_relevance, correctness) computed via
    SYSTEM$EXECUTE_AI_OBSERVABILITY_RUN. Results visible in Snowsight → AI & ML → Evaluations.
    get_results() queries GET_AI_OBSERVABILITY_EVENTS directly for scores + explanations.
        ↓
[RefinementAgent]
    Reads failed questions (correctness < 0.5) + Snowsight explanations.
    Cortex COMPLETE() per table → synonym + description patches (structure never changed).
    Returns None when mean correctness ≥ 0.65 (convergence) or no failed questions remain.
    Loops back to [CortexAnalystProbe].
```

## Ground Truth

Ground truth is computed by querying the actual Snowflake tables directly with known-correct SQL — not by comparing against any hand-crafted YAML. The `examples/streamlit-on-snowflake/manifest/nti_model.yaml` is kept only as a schema reference to understand what columns exist, not as an answer key.

## UI

**Snowsight AI Observability** is the primary interface when using `SnowflakeConnector`. Navigate to `Snowsight → AI & ML → Evaluations`. The Streamlit TruLens dashboard (`run_dashboard()`) is **not supported** with `SnowflakeConnector` — it only works with the default SQLite backend.

Results are also queryable directly from the auto-created Snowflake tables: `TRULENS_RECORDS` and `TRULENS_FEEDBACK_RESULTS`.

A Streamlit in Snowflake wrapper is documented as a future example in `examples/streamlit-on-snowflake/README.md` but is out of scope until the core pipeline works.

## TruLens Integration

The TruLens OSS repo is cloned at `../trulens` (sibling of this repo). Install from the local clone:

```bash
uv add ../trulens/src/core \
       ../trulens/src/feedback \
       ../trulens/src/connectors/snowflake \
       ../trulens/src/providers/cortex
```

Required additional dependency: `snowflake-ml-python>=1.7.1` (needed by the Cortex provider).

### Package map

| pip package | import root | purpose |
|---|---|---|
| `trulens-core` | `trulens.core` | `TruSession`, `Feedback`, `Select` |
| `trulens-feedback` | `trulens.feedback` | `GroundTruthAgreement`, `LLMProvider` base |
| `trulens-connectors-snowflake` | `trulens.connectors.snowflake` | `SnowflakeConnector` — logs to Snowflake |
| `trulens-providers-cortex` | `trulens.providers.cortex` | `Cortex` — 100% Snowflake-native feedback LLM |

### Wiring TruLens to Snowflake

```python
from snowflake.snowpark import Session
from trulens.core import TruSession
from trulens.connectors.snowflake import SnowflakeConnector

connector = SnowflakeConnector(snowpark_session=snowpark_session)
tru_session = TruSession(connector=connector)
# All subsequent TruApp recordings auto-log to Snowflake
```

### Wrapping a custom app (`weaver/evaluator.py` pattern)

Use `TruApp` + `@instrument()` for any Python class that isn't LangChain/LlamaIndex.

```python
from trulens.apps.app import TruApp, instrument

class CortexAnalystApp:
    @instrument()
    def ask(self, question: str) -> str:
        return self.probe.query(question).get("answer", "")

app = CortexAnalystApp(probe)
tru_app = TruApp(app=app, app_name="CortexAnalystProbe", feedbacks=[...])

with tru_app:
    app.ask("이번 달 종목별 거래량 상위 5개는?")
```

### Feedback functions

Use `Cortex` as the feedback provider (no OpenAI key needed, fully Snowflake-native):

```python
from trulens.providers.cortex import Cortex
from trulens.core import Feedback
from trulens.feedback import GroundTruthAgreement

provider = Cortex(snowpark_session=session, model_engine="mistral-large2")

# Answer relevance — does the answer address the question?
f_relevance = Feedback(provider.relevance_with_cot_reasons, name="answer_relevance") \
    .on_input_output()

# Ground truth agreement — answer vs. SQL-derived expected value
golden_set = [{"query": "...", "expected_response": "..."}]  # from ScenarioGenerator
gt = GroundTruthAgreement(golden_set, provider=provider)
f_correctness = Feedback(gt.agreement_measure, name="answer_correctness") \
    .on_input_output()
```

`golden_set` entries must have keys `"query"` and `"expected_response"`. Expected responses are computed by `ScenarioGenerator` running known-correct SQL directly against Snowflake — not via Cortex Analyst.

### Model engine guidance

| Use case | model_engine |
|---|---|
| Iteration / cheap scoring | `"llama3.1-8b"` |
| Final evaluation run | `"mistral-large2"` |

## Benchmark Datasets (Marketplace)

These are the test subjects, not the domain:

| Database | Dataset | What it tests |
|---|---|---|
| `NEXTRADE_EQUITY_MARKET_DATA` | Nextrade — Korean equity market | multi-table joins, time-series measures |
| `KOREAN_POPULATION__APARTMENT_MARKET_PRICE_DATA` | Richgo — apartment prices, migration | geo + price dimensions |
| `SEOUL_DISTRICTLEVEL_DATA_FLOATING_POPULATION_CONSUMPTION_AND_ASSETS` | SPH — floating population, income | wide tables, many measures |
| `SOUTH_KOREA_TELECOM_SUBSCRIPTION_ANALYTICS__CONTRACTS_MARKETING_AND_CALL_CENTER_INSIGHTS_BY_REGION` | 아정당 — telecom contracts | regional FK inference |

## Stack

- **Data platform**: Snowflake (Snowpark, Cortex, Arctic, Cortex Analyst)
- **Evaluation**: TruLens (Snowflake-incubated, logs to Snowflake)
- **Language**: Python (primary), SQL
- **Package manager**: uv (`uv run python -m weaver ...`)

## Conventions

- SQL: uppercase keywords, lowercase identifiers
- Use Snowpark DataFrames over raw SQL in Python where possible
- Secrets and credentials in `.env` (never committed)
- All Snowflake object names in snake_case
- Semantic YAML keys follow the Cortex Analyst spec exactly: `name`, `base_table`, `dimensions`, `measures`, `time_dimensions`, `synonyms`, `relationships`

## Claude Code Setup

Custom agents, commands, hooks, rules, and skills live under `.claude/`.

| Directory          | Purpose                                               |
|--------------------|-------------------------------------------------------|
| `.claude/agents`   | Subagent definitions (SchemaDiscovery, YAMLWriter, etc.) |
| `.claude/commands` | Slash commands for common workflows                   |
| `.claude/hooks`    | Shell hooks triggered by Claude Code events           |
| `.claude/rules`    | Persistent behavioral rules for this project          |
| `.claude/skills`   | Reusable skill prompts                                |
| `.claude/info`     | Hackathon announcements, format, and judging criteria |
| `.claude/whoami`   | Background on the developer (ME.md)                   |

## Querying Snowflake from Claude Code

SnowSQL is installed at `/Applications/SnowSQL.app/Contents/MacOS/snowsql` and configured in `~/.snowsql/config` under the `[connections.hackathon]` profile.

Connection details:
- Account: `ZZTAALY-YA33727`
- User: `EUNSANGLEE`
- Role: `ACCOUNTADMIN`

`SNOWSQL_PWD` is set in `~/.zshrc`. Claude runs queries using the full binary path with the password injected inline:

```bash
SNOWSQL_PWD="..." /Applications/SnowSQL.app/Contents/MacOS/snowsql -c hackathon -q "SELECT ..."
```
