# Example: Streamlit in Snowflake

This folder is a placeholder for a Streamlit in Snowflake front-end that wraps the Semantic Model Weaver pipeline as a Snowflake-native web app.

## Concept

Once `weaver` has generated and evaluated a semantic model, you could expose:
- A database/schema picker (select what to weave)
- A live run log showing the pipeline steps
- The generated YAML with diff vs. previous iteration
- TruLens eval scores per question, per run

## Why it's deferred

The primary UI for Semantic Model Weaver is the **TruLens dashboard** — it provides experiment tracking, per-question scoring, and run comparison out of the box without extra code.

Streamlit in Snowflake is a valid secondary surface for hackathon judges who want a polished in-platform demo, but it's scope that comes *after* the core pipeline works.

## Reference

The original Streamlit + Snowflake Cortex Agent implementation (the pre-pivot NTI app) lived here. It showed how to:
- Connect to Snowflake from Streamlit via `snowflake-connector-python`
- Call the Cortex Analyst REST API from Python
- Stream chat responses back to the UI

That pattern is directly reusable here if/when this example is built out.
