# Urban Financial Radar

> A neighborhood intelligence platform for financial services — powered by Snowflake Cortex Agents.
> Snowflake Hackathon 2026 Seoul

## Tech Stack

| Layer       | Technology                        |
|-------------|-----------------------------------|
| Platform    | Snowflake                         |
| Compute     | Snowpark (Python)                 |
| AI/ML       | Snowflake Cortex / Arctic         |
| Frontend    | Streamlit in Snowflake            |
| Language    | Python, SQL                       |

## Getting Started

```bash
# 1. Copy and fill in credentials
cp .env.example .env

# 2. Install dependencies
pip install -r requirements.txt

# 3. Run locally
streamlit run app.py
```

## Project Structure

```
.
├── CLAUDE.md          # Claude Code project instructions
├── README.md
├── .claude/           # Claude Code configuration
│   ├── agents/        # Subagent definitions
│   ├── commands/      # Slash commands
│   ├── hooks/         # Event hooks
│   ├── rules/         # Behavioral rules
│   └── skills/        # Reusable skills
├── app.py             # Streamlit entrypoint
├── snowpark/          # Snowpark pipeline code
└── sql/               # DDL and seed scripts
```

## Team

- CynicDog
