# Semantic Model Forge

> An agentic pipeline that reads a raw Snowflake database, auto-generates a Cortex Analyst semantic model, tests it against synthesized natural-language scenarios, scores quality via TruLens, and iteratively refines the model — without human authoring.
>
> Snowflake Hackathon 2026 Seoul

## The Problem

Writing a Cortex Analyst semantic YAML by hand is:
- **Manual** — you read the schema, guess at synonyms, and wire joins yourself
- **Blind** — there's no feedback loop; you don't know if the model answers real questions until someone asks them
- **Unverifiable** — there's no metric for "how good is this model"

Semantic Model Forge solves all three.

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
├── forge/
│   ├── __main__.py         # CLI entry: python -m forge --database ... --schema ...
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

# 2. Run the forge against a dataset
uv run python -m forge --database NEXTRADE_EQUITY_MARKET_DATA --schema FIN
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
- A forge-generated model is accepted by the API (200, no error content)
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
