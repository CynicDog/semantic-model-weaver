# Semantic Model Weaver

> An agentic pipeline that reads a raw Snowflake database, auto-generates a Cortex Analyst semantic model, tests it against synthesized natural-language scenarios, scores quality via TruLens, and iteratively refines the model — without human authoring.
>
> Snowflake Hackathon 2026 Seoul

## The Problem

Writing a Cortex Analyst semantic YAML by hand is:
- **Manual** — you read the schema, guess at synonyms, and wire joins yourself
- **Blind** — there's no feedback loop; you don't know if the model answers real questions until someone asks them
- **Unverifiable** — there's no metric for "how good is this model"

Semantic Model Weaver solves all three.

## How It Works

```
[SchemaDiscovery]
    Snowpark reads INFORMATION_SCHEMA, profiles columns,
    samples data, infers FK candidates via name/type matching.
        ↓
[YAMLWriter]
    Cortex Arctic drafts a Cortex Analyst-compliant semantic YAML:
    tables, dimensions, measures, time_dimensions, synonyms, joins.
        ↓
[ScenarioGenerator]
    LLM generates ~20 natural-language questions per table cluster
    that should be answerable by the model (joins, aggregations, filters).
    Also queries raw tables directly to build ground truth answers.
        ↓
[CortexAnalystProbe]
    Cortex Analyst REST API fires each NL question with the draft YAML.
    Captures: SQL generated, execution success, join count, result shape, answer.
        ↓
[EvalLogger + TruLens]
    TruLens (Snowflake-incubated OSS, logs to Snowflake) scores each question:
    groundedness, relevance, answer correctness vs. ground truth.
    Ground truth = direct raw table queries, not a hand-crafted YAML.
    TruLens dashboard is the primary UI.
        ↓
[RefinementAgent]
    Reads the failure report, patches the YAML (missing synonyms,
    wrong joins, unmapped measures), loops back to Probe.
```

## Ground Truth

Ground truth is computed by querying the actual Snowflake tables with known-correct SQL independently of Cortex Analyst. This makes evaluation objective: the generated semantic model either answers correctly against real data or it doesn't.

## UI

The **TruLens dashboard** is the primary interface — experiment tracking, per-question scores, and run comparison out of the box. TruLens is Snowflake-incubated OSS and logs all runs to Snowflake tables.

A Streamlit in Snowflake wrapper is documented in `examples/streamlit-on-snowflake/` as a future polish step.

## Benchmark Datasets

The hackathon Marketplace datasets are the test subjects — not the domain:

| Database | Dataset | What it exercises |
|---|---|---|
| `NEXTRADE_EQUITY_MARKET_DATA` | Nextrade — Korean equity market | multi-table joins, time-series measures |
| `KOREAN_POPULATION__APARTMENT_MARKET_PRICE_DATA` | Richgo — apartment prices, migration | geo + price dimensions |
| `SEOUL_DISTRICTLEVEL_DATA_FLOATING_POPULATION_CONSUMPTION_AND_ASSETS` | SPH — floating population, income | wide tables, many measures |
| `SOUTH_KOREA_TELECOM_SUBSCRIPTION_ANALYTICS__CONTRACTS_MARKETING_AND_CALL_CENTER_INSIGHTS_BY_REGION` | 아정당 — telecom contracts | regional FK inference |

## Tech Stack

| Layer | Technology |
|---|---|
| Schema discovery | Snowflake Snowpark (`INFORMATION_SCHEMA`) |
| Model generation | Snowflake Cortex Arctic |
| Semantic query testing | Cortex Analyst REST API |
| Evaluation & scoring | TruLens (Snowflake-incubated, logs to Snowflake) |
| Language | Python |
| Package manager | uv |

## Project Structure

```
.
├── pyproject.toml
├── CLAUDE.md
├── README.md
├── weaver/
│   ├── __main__.py         # CLI entry: python -m weaver --database ... --schema ...
│   ├── discovery.py        # SchemaDiscovery — Snowpark schema profiling
│   ├── writer.py           # YAMLWriter — LLM-driven semantic YAML generation
│   ├── scenarios.py        # ScenarioGenerator — NL question + ground truth synthesis
│   ├── probe.py            # CortexAnalystProbe — REST API test harness
│   ├── logger.py           # EvalLogger — metric collection
│   ├── evaluator.py        # TruLens scoring
│   └── refiner.py          # RefinementAgent — YAML patching loop
└── examples/
    └── streamlit-on-snowflake/
        ├── README.md           # Future: Streamlit in Snowflake wrapper
        └── manifest/
            └── nti_model.yaml  # Hand-crafted semantic YAML (pre-pivot NTI app)
```

## Getting Started

```bash
# 1. Fill in credentials
cp .env.example .env
# Edit .env — set WEAVER_SNOWFLAKE_* and WEAVER_ALLOWED_IP (your outbound IP)
# Find your IP with: curl -s https://checkip.amazonaws.com
```

## Usage

```bash
# First time — create workspace database, schema, and network policy
uv run python -m weaver --setup

# Korean equity market (Nextrade)
uv run python -m weaver --database NEXTRADE_EQUITY_MARKET_DATA --schema FIN

# Apartment prices and population (Richgo)
uv run python -m weaver --database KOREAN_POPULATION__APARTMENT_MARKET_PRICE_DATA --schema HACKATHON_2025Q2

# Floating population, consumption, and assets (SPH)
uv run python -m weaver --database SEOUL_DISTRICTLEVEL_DATA_FLOATING_POPULATION_CONSUMPTION_AND_ASSETS --schema GRANDATA

# Telecom subscription analytics (아정당)
uv run python -m weaver --database SOUTH_KOREA_TELECOM_SUBSCRIPTION_ANALYTICS__CONTRACTS_MARKETING_AND_CALL_CENTER_INSIGHTS_BY_REGION --schema TELECOM_INSIGHTS

# Run with custom iteration count and version tag
uv run python -m weaver --database NEXTRADE_EQUITY_MARKET_DATA --schema FIN --iterations 5 --version v2

# Resume a previous run from a checkpoint directory (stage auto-detected from artifacts)
uv run python -m weaver --resume manifest/NEXTRADE_EQUITY_MARKET_DATA.FIN/20260406_202959/

# Clear all evaluation records and start fresh
uv run python -m weaver --reset-workspace
uv run python -m weaver --reset-workspace --yes   # skip confirmation prompt
```

View results in **Snowsight → AI & ML → Evaluations** after a run completes.

## Chat Apps

Each dataset has a standalone Streamlit chat app that sends the generated semantic YAML to Cortex Analyst and renders results as interactive tables and charts.

Copy the final model from a pipeline run into the app directory first:

```bash
cp manifest/NEXTRADE_EQUITY_MARKET_DATA.FIN/<timestamp>/model.final.yaml                                                                    examples/nextrade/model.yaml
cp manifest/KOREAN_POPULATION__APARTMENT_MARKET_PRICE_DATA.HACKATHON_2025Q2/<timestamp>/model.final.yaml                                    examples/korean_population/model.yaml
cp manifest/SEOUL_DISTRICTLEVEL_DATA_FLOATING_POPULATION_CONSUMPTION_AND_ASSETS.GRANDATA/<timestamp>/model.final.yaml                       examples/seoul_floating_population/model.yaml
cp manifest/SOUTH_KOREA_TELECOM_SUBSCRIPTION_ANALYTICS__CONTRACTS_MARKETING_AND_CALL_CENTER_INSIGHTS_BY_REGION.TELECOM_INSIGHTS/<timestamp>/model.final.yaml  examples/telecom/model.yaml
```

Then run any app:

```bash
# 📈 넥스트레이드 — 한국 주식시장 데이터
uv run streamlit run examples/nextrade/app.py
       
# 🏘️ Richgo — 한국 아파트 시세 및 인구 이동 데이터
uv run streamlit run examples/korean_population/app.py

# 🗺️ SPH / GranData — 서울 생활인구·소비·자산 데이터
uv run streamlit run examples/seoul_floating_population/app.py

# 📡 아정당 — 통신 계약·마케팅·콜센터 지역별 분석 데이터
uv run streamlit run examples/telecom/app.py
```

Or override the model path without copying:

```bash
WEAVER_MODEL_YAML=manifest/NEXTRADE_EQUITY_MARKET_DATA.FIN/<timestamp>/model.final.yaml \
  uv run --project examples/nextrade streamlit run examples/nextrade/app.py
```

### Running in containers

Pre-built images are published to GitHub Container Registry on every push to `main`.

Pull and run any chat app (replace `<app>` with `nextrade`, `korean-population`, `seoul-floating-population`, or `telecom`):

```bash
docker run -p 8501:8501 \
  -e WEAVER_SNOWFLAKE_ACCOUNT=<your_account> \
  -e WEAVER_SNOWFLAKE_USER=<your_user> \
  -e WEAVER_SNOWFLAKE_PASSWORD=<your_password> \
  -e WEAVER_SNOWFLAKE_ROLE=ACCOUNTADMIN \
  -e WEAVER_SNOWFLAKE_WAREHOUSE=COMPUTE_WH \
  ghcr.io/cynicdog/semantic-model-weaver/<app>:latest
```

Then open `http://localhost:8501` in your browser.

Run the weaver pipeline CLI from a container:

```bash
docker run --rm \
  -e WEAVER_SNOWFLAKE_ACCOUNT=<your_account> \
  -e WEAVER_SNOWFLAKE_USER=<your_user> \
  -e WEAVER_SNOWFLAKE_PASSWORD=<your_password> \
  -e WEAVER_SNOWFLAKE_ROLE=ACCOUNTADMIN \
  -e WEAVER_SNOWFLAKE_WAREHOUSE=COMPUTE_WH \
  -e WEAVER_SNOWFLAKE_DATABASE=semantic_model_weaver \
  -e WEAVER_SNOWFLAKE_SCHEMA=trulens \
  -v $(pwd)/manifest:/app/manifest \
  ghcr.io/cynicdog/semantic-model-weaver/weaver-cli:latest \
  --database NEXTRADE_EQUITY_MARKET_DATA --schema FIN
```

Mount `./manifest` so pipeline output (YAML snapshots, scenarios) is persisted to your host.

Build images locally:

```bash
# Weaver CLI
docker build -t weaver-cli .

# Any chat app (build context is the example directory)
docker build -t nextrade examples/nextrade
docker build -t korean-population examples/korean_population
docker build -t seoul-floating-population examples/seoul_floating_population
docker build -t telecom examples/telecom
```

## Testing

Two layers — unit tests run anywhere, integration tests require a live Snowflake session.

### Unit tests

No Snowflake connection needed. Validates DSL structure, serialisation, and all validators.

```bash
uv run pytest tests/test_dsl.py -v
```

Covers:
- Parsing `examples/streamlit-on-snowflake/manifest/nti_model.yaml` (real Cortex Analyst YAML)
- Round-trip fidelity: `from_yaml(to_yaml(model))` produces an equivalent model
- YAML output rules: `schema` alias, empty lists/strings stripped, no `default_aggregation`
- Validators: `TimeDimension` rejects non-temporal types, `Relationship` rejects dangling table refs

### Integration tests

Posts generated YAML to the real Cortex Analyst REST API. Requires `SNOWSQL_PWD` in the environment (already set in `~/.zshrc`).

```bash
uv run pytest tests/test_cortex_analyst_api.py -v -m integration
```

Covers:
- A weaver-generated model is accepted by the API (200, no error content)
- A simple count question returns an `sql` response type
- Deliberately malformed YAML triggers an API error — proving acceptance tests are meaningful
- `nti_model.yaml` survives a DSL round-trip and is still accepted by the API

### Run everything

```bash
uv run pytest -v                    # unit only (integration skipped without credentials)
uv run pytest -v -m integration     # integration only
uv run pytest -v -m "not integration" # explicitly unit only
```

## Team

- CynicDog (Eunsang Lee) — Data Engineer, MetLife Korea
