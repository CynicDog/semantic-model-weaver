# CLAUDE.md

This file provides guidance to Claude Code when working in this repository.

## Project

Urban Financial Radar — Snowflake Hackathon 2026 Seoul

## Stack

- **Data platform**: Snowflake (Snowpark, Cortex, Arctic, Streamlit in Snowflake)
- **Language**: Python (primary), SQL
- **Infra/deploy**: Snowflake Native App / Streamlit in Snowflake

## Conventions

- Keep SQL uppercase for keywords, lowercase for identifiers
- Use Snowpark DataFrames over raw SQL where possible in Python
- Secrets and credentials go in `.env` (never committed)
- All Snowflake object names use snake_case

## Claude Code setup

Custom agents, commands, hooks, rules, and skills live under `.claude/`.

| Directory        | Purpose                                      |
|------------------|----------------------------------------------|
| `.claude/agents` | Subagent definitions for specialized tasks   |
| `.claude/commands` | Slash commands for common workflows        |
| `.claude/hooks`  | Shell hooks triggered by Claude Code events  |
| `.claude/rules`  | Persistent behavioral rules for this project |
| `.claude/skills`        | Reusable skill prompts                       |
| `.claude/info`          | Hackathon announcements, format, and judging criteria |
| `.claude/whoami`        | Background on the developer (ME.md)                  |

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

### Databases available (Marketplace)

| Database | Dataset |
|---|---|
| `KOREAN_POPULATION__APARTMENT_MARKET_PRICE_DATA` | Richgo — apartment prices, migration |
| `NEXTRADE_EQUITY_MARKET_DATA` | Nextrade — Korean equity market |
| `SEOUL_DISTRICTLEVEL_DATA_FLOATING_POPULATION_CONSUMPTION_AND_ASSETS` | SPH — floating population, income, consumption |
| `SOUTH_KOREA_TELECOM_SUBSCRIPTION_ANALYTICS__CONTRACTS_MARKETING_AND_CALL_CENTER_INSIGHTS_BY_REGION` | 아정당 — telecom contracts, relocation, rental |
