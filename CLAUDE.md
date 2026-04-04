# CLAUDE.md

This file provides guidance to Claude Code when working in this repository.

## Project

**Semantic Model Forge** — Snowflake Hackathon 2026 Seoul

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
    samples data, infers FK candidates via column name/type matching.
        ↓
[YAMLWriter]
    Cortex Arctic — drafts a Cortex Analyst-compliant semantic YAML:
    tables, dimensions, measures, time_dimensions, synonyms, joins.
        ↓
[ScenarioGenerator]
    LLM — generates ~20 natural-language questions per table cluster
    that *should* be answerable by the model (joins, aggregations, filters).
    Also independently queries raw tables to compute ground truth answers.
        ↓
[CortexAnalystProbe]
    Cortex Analyst REST API — fires each NL question with the draft YAML.
    Captures: SQL generated, execution success, join count, result shape, answer.
        ↓
[EvalLogger + TruLens]
    TruLens (Snowflake-incubated OSS, logs to Snowflake backend) — scores each
    question on groundedness, relevance, and answer correctness vs. ground truth.
    Ground truth = answers derived directly from raw table queries, not from any
    hand-crafted YAML. TruLens dashboard is the primary UI.
        ↓
[RefinementAgent]
    Reads the failure report, patches the YAML (missing synonyms, wrong joins,
    unmapped measures), loops back to [CortexAnalystProbe].
```

## Ground Truth

Ground truth is computed by querying the actual Snowflake tables directly with known-correct SQL — not by comparing against any hand-crafted YAML. The `examples/streamlit-on-snowflake/manifest/nti_model.yaml` is kept only as a schema reference to understand what columns exist, not as an answer key.

## UI

**TruLens dashboard** is the primary interface. TruLens is Snowflake-incubated OSS with a built-in experiment tracking UI, per-question scoring, and run comparison — no Streamlit needed for the core app.

A Streamlit in Snowflake wrapper is documented as a future example in `examples/streamlit-on-snowflake/README.md` but is out of scope until the core pipeline works.

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
- **Package manager**: uv (`uv run python -m forge ...`)

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
