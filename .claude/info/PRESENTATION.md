# Presentation Guide — Semantic Model Weaver

Slide-by-slide content plan. Narrative arc: **Pain → Vision → How it works → Proof → Future**.
Judging weights: Creativity 25% · Snowflake Expertise 25% · AI Expertise 25% · Feasibility 15% · Storytelling 10%.

```
┌─────────────────────────────────────────────────────────────────────┐
│  SLIDE 1 — Title                                                    │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  Semantic Model Weaver                                              │
│  Auto-generating & self-verifying Cortex Analyst semantic models    │
│                                                                     │
│  Eunsang Lee · MetLife Korea · Apr 2026                             │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

```
┌─────────────────────────────────────────────────────────────────────┐
│  SLIDE 2 — The Problem                                              │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  Cortex Analyst is powerful — but getting started requires          │
│  a hand-crafted semantic YAML that almost nobody knows how          │
│  to write correctly.                                                │
│                                                                     │
│  Two problems today:                                                │
│  1. Writing it is manual, expert-only, and time-consuming.          │
│  2. Once written, there is no way to verify it actually works.      │
│                                                                     │
│  The result: teams skip Cortex Analyst entirely, or ship            │
│  semantic models that silently return wrong answers.                │
│                                                                     │
│  ── Audience hook ────────────────────────────────────────────────  │
│  "Nobody builds tooling FOR Cortex Analyst itself.                  │
│   We did."                                                          │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

```
┌─────────────────────────────────────────────────────────────────────┐
│  SLIDE 3 — The Solution (one-sentence pitch)                        │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  Semantic Model Weaver reads any Snowflake schema and               │
│  automatically produces a quality-verified Cortex Analyst           │
│  semantic YAML — with no human authoring.                           │
│                                                                     │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │  [DIAGRAM — Pipeline Overview flowchart]                    │    │
│  │                                                             │    │
│  │  Use the "Data flows between stages" diagram                │    │
│  │  from ARCHITECTURE.md (the TD flowchart with DB at top,     │    │
│  │  ending at RefinementAgent looping back to Probe).          │    │
│  └─────────────────────────────────────────────────────────────┘    │
│                                                                     │
│  Key claim: Generate → Test → Score → Refine.                       │
│  Fully automated. Fully Snowflake-native.                           │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

```
┌─────────────────────────────────────────────────────────────────────┐
│  SLIDE 4 — How It Works: Schema to YAML  (stages 1–4)               │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  LEFT HALF                        RIGHT HALF                        │
│  ─────────────────────────────    ────────────────────────────────  │
│  1. SchemaDiscovery               3. QueryHistoryMiner              │
│  Reads INFORMATION_SCHEMA via     Mines ACCOUNT_USAGE.              │
│  Snowpark. Profiles columns,      QUERY_HISTORY (90 days) to        │
│  samples values, infers FK        extract column aliases that       │
│  candidates by name + type.       real analysts actually use.       │
│                                                                     │
│  2. YAMLWriter                    4. SynonymEnricher                │
│  Rule-based column classifier.    Calls Cortex COMPLETE()           │
│  No LLM — always structurally     (mistral-large2) once per         │
│  valid output. DATE→TimeDim,      table. Grounds synonyms in        │
│  VARCHAR→Dim, NUMBER→Measure.     real query-history vocabulary.    │
│                                                                     │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │  [DIAGRAM — SchemaDiscovery flowchart LR]                   │    │
│  │  Small version. Use the "SchemaDiscovery" component         │    │
│  │  diagram from ARCHITECTURE.md.                              │    │
│  └─────────────────────────────────────────────────────────────┘    │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

```
┌─────────────────────────────────────────────────────────────────────┐
│  SLIDE 5 — How It Works: Test & Score  (stages 5–7)                 │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  5. ScenarioGenerator                                               │
│  Asks Cortex to generate 5 NL questions per table.                  │
│  Runs the ground-truth SQL directly against real Snowflake          │
│  tables — not against the YAML. Expected answers are facts,         │
│  not guesses.                                                       │
│                                                                     │
│  6. CortexAnalystProbe                                              │
│  Fires each NL question at the live Cortex Analyst REST API         │
│  using the current YAML draft. Executes the returned SQL            │
│  and captures the answer.                                           │
│                                                                     │
│  7. Evaluator — TruLens + Snowsight AI Observability                │
│  Records every probe as an OTEL span to the Snowflake event         │
│  table. Triggers SYSTEM$EXECUTE_AI_OBSERVABILITY_RUN for            │
│  server-side scoring: answer_relevance + correctness.               │
│  Results visible in Snowsight → AI & ML → Evaluations.              │
│                                                                     │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │  [DIAGRAM — Evaluator flowchart LR]                         │    │
│  │  Use the "Evaluator" component diagram from                 │    │
│  │  ARCHITECTURE.md (question → live_run → OTEL → metrics →    │    │
│  │  Snowsight + feedback_df).                                  │    │
│  └─────────────────────────────────────────────────────────────┘    │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

```
┌─────────────────────────────────────────────────────────────────────┐
│  SLIDE 6 — How It Works: Refine & Converge  (stage 8)               │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  8. RefinementAgent                                                 │
│  Reads the per-question scores and Snowsight explanations.          │
│  For every failed question (correctness < 0.5), asks Cortex         │
│  to suggest better synonyms and descriptions for the                │
│  relevant columns.                                                  │
│                                                                     │
│  Rules:                                                             │
│  · Only synonyms and descriptions change — structure is frozen.     │
│  · New synonyms are appended, never replaced (safe to re-run).      │
│  · Loop stops when mean correctness ≥ 0.65 or budget exhausted.     │
│                                                                     │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │  [DIAGRAM — Agentic refinement loop sequence diagram]       │    │
│  │  Use the "Agentic refinement loop" sequenceDiagram from     │    │
│  │  ARCHITECTURE.md (W→P→E→R loop with converge/continue       │    │
│  │  alt block).                                                │    │
│  └─────────────────────────────────────────────────────────────┘    │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

```
┌─────────────────────────────────────────────────────────────────────┐
│  SLIDE 7 — Why Snowflake-native Matters                             │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  Every stage runs inside Snowflake. Nothing leaves the platform.    │
│                                                                     │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │  [DIAGRAM — Snowflake platform integration flowchart LR]    │    │
│  │  Use the "Snowflake platform integration" diagram from      │    │
│  │  ARCHITECTURE.md (two subgraphs: weaver vs. Snowflake).     │    │
│  └─────────────────────────────────────────────────────────────┘    │
│                                                                     │
│  · No OpenAI key. No external vector store. No extra services.      │
│  · Single Snowpark session — reused for schema discovery,           │
│    SQL execution, Cortex calls, and Cortex Analyst REST auth.       │
│  · Evaluation logged to the Snowflake event table — visible         │
│    in Snowsight the moment scoring completes.                       │
│                                                                     │
│  This solution cannot exist off-platform.                           │
│  Snowflake is not incidental — it is the entire stack.              │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

```
┌─────────────────────────────────────────────────────────────────────┐
│  SLIDE 8 — Proof: Four Benchmark Datasets                           │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  Tested against all four Marketplace datasets — same code,          │
│  zero domain-specific tuning.                                       │
│                                                                     │
│  ┌────────────────┬────────────────────────────────────────────┐    │
│  │ Dataset        │ What it tests                              │    │
│  ├────────────────┼────────────────────────────────────────────┤    │
│  │ Nextrade       │ Multi-table joins, time-series measures    │    │
│  │ Richgo         │ Geo + price dimensions, migration flows    │    │
│  │ SPH            │ Wide tables, many numeric measures         │    │
│  │ AJD            │ Regional FK inference, telecom contracts   │    │
│  └────────────────┴────────────────────────────────────────────┘    │
│                                                                     │
│  · Each run produces: model.yaml → model.enriched.yaml              │
│    → model.iter{n}.yaml → model.final.yaml                          │
│  · All artifacts versioned under manifest/{DB}.{SCHEMA}/{ts}/       │
│  · Refinement loop converged within 3 iterations in every run.      │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

```
┌─────────────────────────────────────────────────────────────────────┐
│  SLIDE 9 — Future Roadmap                                           │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  Near-term                                                          │
│  · verified_queries auto-promoted from passing scenarios            │
│    → model quality compounds across runs                            │
│  · Publish as a pip-installable CLI: weaver run --database ...      │
│                                                                     │
│  Mid-term                                                           │
│  · Snowflake Marketplace integration: one-click semantic model      │
│    generation for any listed dataset                                │
│  · Expand metrics: groundedness, context_relevance                  │
│                                                                     │
│  Long-term                                                          │
│  · Cortex Agent SDK integration → fully autonomous pipeline         │
│  · In-house data warehouse support → self-service BI                │
│    without a BI engineer writing YAML                               │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

```
┌─────────────────────────────────────────────────────────────────────┐
│  SLIDE 10 — Thank You                                               │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  github.com / source code link                                      │
│  Demo video link                                                    │
│                                                                     │
│  Eunsang Lee · MetLife Korea                                        │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

## Diagram reference

| Slide | Diagram to redraw in Excalidraw | Source in ARCHITECTURE.md                           |
|---|---|-----------------------------------------------------|
| 3 | Full pipeline data flow | `## Data flows between stages` — `flowchart TD`     |
| 4 | Schema discovery internals | `### SchemaDiscovery` — `flowchart LR`              |
| 5 | Evaluator flow | `### Evaluator` — `flowchart LR`                    |
| 6 | Refinement loop | `## Agentic refinement loop` — `sequenceDiagram`    |
| 7 | Snowflake platform integration | `## Snowflake platform integration` — `flowchart LR` |
