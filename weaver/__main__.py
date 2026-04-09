"""
Entry point: uv run python -m weaver [action] [options]

Actions
-------
  --setup                  Create workspace DB + schema + network policy. Safe to re-run.
  --reset-workspace        Drop and recreate the TruLens schema. Clears all evaluation
                           records. Requires confirmation unless --yes is passed.
  --database X --schema Y  Run the full pipeline against a Snowflake dataset (default).
  --iterations N           Max refinement iterations (default: 3).
  --version TAG            TruLens app version tag (default: v1).
  --resume RUN_DIR         Resume from a previous run directory. Stage is auto-detected
                           from artifacts present in the directory.

Pipeline stages
---------------
  [SchemaDiscovery]      → SchemaProfile            weaver/discovery.py
      Snowpark reads INFORMATION_SCHEMA; samples text/boolean columns;
      infers FK candidates by name + type-family matching.

  [YAMLWriter]           → SemanticModel            weaver/writer.py
      Rule-based (no LLM): classifies columns into dimensions / measures /
      time_dimensions by type and name-suffix heuristics; builds relationships
      from FK candidates.

  [QueryHistoryMiner]    → QueryTerms               weaver/query_history.py
      Mines SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY (90-day lookback, ≤1000
      queries); extracts column aliases from historical SQL to build a business
      vocabulary map {table → {col → [alias phrases]}}.

  [SynonymEnricher]      → SemanticModel.enriched   weaver/enricher.py
      Cortex COMPLETE() (mistral-large2) per table; descriptions + synonyms
      grounded by QueryTerms; raw-value synonyms and type-echo descriptions
      are filtered before applying.

  [ScenarioGenerator]    → golden_set, questions    weaver/scenarios.py
      Cortex COMPLETE() per table; 5 NL questions per table including joins;
      ground-truth SQL executed directly against Snowflake via Snowpark.

  loop until correctness >= 0.65 or iteration budget exhausted:
    [CortexAnalystProbe]   → ProbeResult per question  weaver/probe.py
        Cortex Analyst REST API; session-token auth; executes returned SQL
        and formats top-5 rows as the answer string.

    [Evaluator + TruLens]  → feedback_df               weaver/evaluator.py
        TruLens live_run -> OTEL spans -> Snowflake event table; server-side
        metrics (answer_relevance, correctness) via
        SYSTEM$EXECUTE_AI_OBSERVABILITY_RUN; results in Snowsight AI & ML ->
        Evaluations.

    [VerifiedQuery promotion]
        Questions with correctness >= 0.80 and a non-empty SQL are promoted
        into SemanticModel.verified_queries to anchor future Cortex Analyst
        SQL generation.

    [RefinementAgent]      → SemanticModel (patched)   weaver/refiner.py
        Cortex COMPLETE() per table; patches synonyms + descriptions only
        (structure never changed); returns None on convergence.

Checkpoint / resume
-------------------
  Artifacts are written to manifest/{DATABASE}.{SCHEMA}/{timestamp}/ after
  each stage. --resume auto-detects the stage from whichever artifacts exist:
    scenarios.json present          -> jump straight to evaluation loop
    synonyms.json present           -> jump to scenario generation
    model.yaml present              -> jump to enrichment
"""

import logging
import os
import re
import warnings

from dotenv import load_dotenv
from rich.console import Console

warnings.filterwarnings(
    "ignore",
    message="Bad owner or permissions on.*connections.toml",
    category=UserWarning,
)

console = Console()
log = logging.getLogger(__name__)

_DB = lambda: os.environ.get("WEAVER_SNOWFLAKE_DATABASE", "semantic_model_weaver")  # noqa: E731
_SCHEMA = lambda: os.environ.get("WEAVER_SNOWFLAKE_SCHEMA", "trulens")  # noqa: E731

_BANNER = """\
 Y8b Y8b Y888P  ,e e,   ,"Y88b Y8b Y888P  ,e e,  888,8,
  Y8b Y8b Y8P  d88 88b "8" 888  Y8b Y8P  d88 88b 888 "
   Y8b Y8b "   888   , ,ee 888   Y8b "   888   , 888
    YP  Y8P     "YeeP" "88 888    Y8P     "YeeP" 888    """


def _print_banner():
    console.print()
    console.print(_BANNER, style="bold cyan", highlight=False)
    console.print()
    console.print("  [bold white]Semantic Model Weaver[/bold white]  [dim]·  Snowflake Hackathon 2026 Seoul  ·  @CynicDog[/dim]")
    console.rule(style="cyan")
    console.print()


def _ok(msg: str):
    console.print(f"  [bold green]✓[/bold green]  {msg}")


def _info(msg: str):
    console.print(f"  [cyan]→[/cyan]  {msg}")


def _warn(msg: str):
    console.print(f"  [yellow]![/yellow]  {msg}")


_PIPELINE_FACES = ["(-.-)", "(o.o)", "(^.^)", "(~_~)", "(*.*)", "(^.^)", "(o.o)", "(-_-)"]


class _PipelineDisplay:
    """
    Rich Live renderable: shows all pipeline steps upfront.

    Pending steps are grayed out. The active step spins with an ASCII face
    and shows elapsed time + overall progress %. Done steps get a ✓.
    """

    _BAR_W = 16

    def __init__(self, steps: list[tuple[str, str]]) -> None:
        self._steps = steps
        self._total = len(steps)
        self._status: dict[str, str] = {k: "pending" for k, _ in steps}
        self._result: dict[str, str] = {}
        self._done = 0
        self._frame = 0
        self._t0: dict[str, float] = {}
        self._duration: dict[str, float] = {}

    def start(self, key: str) -> None:
        import time
        self._status[key] = "active"
        self._t0[key] = time.monotonic()

    def complete(self, key: str, result: str = "") -> None:
        import time
        self._status[key] = "done"
        self._result[key] = result
        self._done += 1
        self._duration[key] = time.monotonic() - self._t0.get(key, time.monotonic())

    def __rich__(self) -> object:
        import time
        from rich.console import Group
        from rich.text import Text

        self._frame = (self._frame + 1) % len(_PIPELINE_FACES)
        face = _PIPELINE_FACES[self._frame]
        pct = int(self._done / self._total * 100) if self._total else 0
        filled = round(pct / 100 * self._BAR_W)
        bar = "━" * filled + ("╸" if filled < self._BAR_W else "") + "─" * max(0, self._BAR_W - filled - 1)

        lines: list[Text] = [Text()]
        for key, label in self._steps:
            s = self._status[key]
            res = self._result.get(key, "")
            t = Text()
            if s == "done":
                t.append("  ✓  ", style="bold green")
                t.append(label, style="white")
                dur = self._duration.get(key, 0)
                if res:
                    t.append(f"  {res}", style="dim")
                t.append(f"  {dur:.0f}s", style="dim")
            elif s == "active":
                elapsed = time.monotonic() - self._t0.get(key, time.monotonic())
                t.append(f"  {face}  ", style="bold cyan")
                t.append(label, style="bold white")
                t.append(f"  [{bar}] {pct}%  {elapsed:.0f}s", style="cyan")
            else:
                t.append("  ·  ", style="dim")
                t.append(label, style="dim")
            lines.append(t)

        lines.append(Text())
        return Group(*lines)



def _session(with_database: bool = True):
    from snowflake.snowpark import Session

    cfg = {
        "account": os.environ["WEAVER_SNOWFLAKE_ACCOUNT"],
        "user": os.environ["WEAVER_SNOWFLAKE_USER"],
        "password": os.environ["WEAVER_SNOWFLAKE_PASSWORD"],
        "role": os.environ.get("WEAVER_SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
        "warehouse": os.environ.get("WEAVER_SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
    }
    if with_database:
        cfg["database"] = _DB()
        cfg["schema"] = _SCHEMA()
    return Session.builder.configs(cfg).create()


def _setup():
    db = _DB()
    schema = _SCHEMA()

    _info(f"connecting to Snowflake account [bold]{os.environ.get('WEAVER_SNOWFLAKE_ACCOUNT')}[/bold]")
    session = _session(with_database=False)

    session.sql(f"""
        CREATE DATABASE IF NOT EXISTS {db}
        COMMENT = 'Semantic Model Weaver — evaluation runs and TruLens records'
    """).collect()
    _ok(f"database [bold]{db}[/bold]")

    session.sql(f"""
        CREATE SCHEMA IF NOT EXISTS {db}.{schema}
        COMMENT = 'TruLens evaluation logs: records, feedback results'
    """).collect()
    _ok(f"schema [bold]{db}.{schema}[/bold]")

    role = os.environ.get("WEAVER_SNOWFLAKE_ROLE", "ACCOUNTADMIN")
    session.sql(f"""
        GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE {role}
    """).collect()
    _ok(f"Cortex usage granted to role [bold]{role}[/bold]")

    allowed_ip = "0.0.0.0/0" # os.environ.get("WEAVER_ALLOWED_IP", "0.0.0.0/0")
    user = os.environ["WEAVER_SNOWFLAKE_USER"]
    _info("refreshing network policy …")
    session.sql(f"ALTER USER {user} UNSET NETWORK_POLICY").collect()
    session.sql("DROP NETWORK POLICY IF EXISTS weaver_dev").collect()
    session.sql(f"DROP NETWORK RULE IF EXISTS {db}.public.weaver_dev_ip").collect()
    session.sql(f"""
        CREATE NETWORK RULE {db}.public.weaver_dev_ip
            TYPE       = ipv4
            VALUE_LIST = ('{allowed_ip}')
            MODE       = ingress
            COMMENT    = 'Dev machine IP for Semantic Model Weaver local runs'
    """).collect()
    session.sql(f"""
        CREATE NETWORK POLICY weaver_dev
            ALLOWED_NETWORK_RULE_LIST = ('{db}.public.weaver_dev_ip')
            COMMENT = 'Network policy for Semantic Model Weaver development'
    """).collect()
    session.sql(f"ALTER USER {user} SET NETWORK_POLICY = weaver_dev").collect()
    _ok(f"network policy [bold]weaver_dev[/bold] → user [bold]{user}[/bold]  [dim](ip: {allowed_ip})[/dim]")

    console.print()
    console.print("  [bold green]Setup complete.[/bold green]  Workspace is ready.")
    console.print()


def _reset_workspace(yes: bool):
    db = _DB()
    schema = _SCHEMA()

    if not yes:
        console.print(f"\n  [bold yellow]Warning:[/bold yellow] This will drop [bold]{db}.{schema}[/bold] and all TruLens records.")
        answer = console.input("  Type [bold]yes[/bold] to confirm: ")
        if answer.strip().lower() != "yes":
            _info("aborted")
            return

    _info(f"connecting to Snowflake …")
    session = _session(with_database=False)

    session.sql(f"DROP SCHEMA IF EXISTS {db}.{schema} CASCADE").collect()
    _ok(f"dropped [bold]{db}.{schema}[/bold]")

    session.sql(f"""
        CREATE SCHEMA {db}.{schema}
        COMMENT = 'TruLens evaluation logs: records, feedback results'
    """).collect()
    _ok(f"recreated [bold]{db}.{schema}[/bold]")

    console.print()
    console.print("  [bold green]Workspace reset.[/bold green]  All evaluation records cleared.")
    console.print()


def _run_dir(database: str, schema: str, timestamp: str) -> "pathlib.Path":
    import pathlib
    d = pathlib.Path("manifest") / f"{database}.{schema}" / timestamp
    d.mkdir(parents=True, exist_ok=True)
    return d


def _dump_yaml(run_dir, semantic_model, suffix: str = "") -> str:
    path = run_dir / f"model{suffix}.yaml"
    path.write_text(semantic_model.to_yaml(), encoding="utf-8")
    return str(path)


def _dump_scenarios(run_dir, golden_set: list, questions: list) -> str:
    import json
    path = run_dir / "scenarios.json"
    path.write_text(
        json.dumps({"golden_set": golden_set, "questions": questions}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return str(path)


def _dump_synonyms(run_dir, semantic_model, suffix: str = "") -> str:
    import json
    path = run_dir / f"synonyms{suffix}.json"
    data = {}
    for t in semantic_model.tables:
        cols = {}
        for col in [*t.dimensions, *t.time_dimensions, *t.measures]:
            if col.synonyms:
                cols[col.name] = col.synonyms
        if cols:
            data[t.name] = cols
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    return str(path)


def _promote_verified_queries(
    semantic_model: "SemanticModel",
    feedback_df: "Any",
    probe_results: dict,
    version: str,
    threshold: float = 0.8,
) -> "SemanticModel":
    """
    Promote high-correctness question+SQL pairs into SemanticModel.verified_queries.

    A question is promoted when its correctness score is >= threshold AND the probe
    returned a non-empty SQL string for it. Already-verified questions (by question
    text) are not duplicated.
    """
    import datetime

    from weaver.dsl import VerifiedQuery

    if feedback_df.empty or "correctness" not in feedback_df.columns or "input" not in feedback_df.columns:
        return semantic_model, 0

    passing = feedback_df[feedback_df["correctness"] >= threshold]
    if passing.empty:
        return semantic_model, 0

    existing_questions = {vq.question for vq in semantic_model.verified_queries}
    now = int(datetime.datetime.now(datetime.UTC).timestamp())
    new_vqs = list(semantic_model.verified_queries)

    for _, row in passing.iterrows():
        question = row["input"]
        if question in existing_questions:
            continue
        result = probe_results.get(question, {})
        sql = result.get("sql") or ""
        if not sql:
            continue
        slug = re.sub(r"[^a-z0-9]+", "_", question.lower())[:48].strip("_")
        new_vqs.append(VerifiedQuery(
            name=slug,
            question=question,
            sql=sql,
            verified_at=now,
            verified_by=f"weaver/{version}",
        ))
        existing_questions.add(question)

    added = len(new_vqs) - len(semantic_model.verified_queries)
    return semantic_model.model_copy(update={"verified_queries": new_vqs}), added


def _best_model_yaml(run_dir: "pathlib.Path") -> "pathlib.Path | None":
    """Return the most refined model YAML in run_dir (final > iter{N} > enriched > base)."""
    import pathlib
    if (run_dir / "model.final.yaml").exists():
        return run_dir / "model.final.yaml"
    iter_models = sorted(
        run_dir.glob("model.iter*.yaml"),
        key=lambda p: int(p.stem.split("iter")[1]) if p.stem.split("iter")[1].isdigit() else 0,
    )
    if iter_models:
        return iter_models[-1]
    for name in ("model.enriched.yaml", "model.yaml"):
        if (run_dir / name).exists():
            return run_dir / name
    return None


def _detect_checkpoint(run_dir: "pathlib.Path") -> dict:
    """
    Scan a run dir and return what stage to resume from.

    Stages:
      "enrichment"  — model.yaml exists; re-run enrichment → scenarios → evaluation
      "scenarios"   — model.enriched.yaml + synonyms.json; skip to scenario generation
      "evaluation"  — scenarios.json + any model YAML; skip straight to evaluation loop
    """
    model_path = _best_model_yaml(run_dir)
    scenarios_path = run_dir / "scenarios.json"

    if scenarios_path.exists() and model_path:
        return {"stage": "evaluation", "model_path": model_path, "scenarios_path": scenarios_path}

    enriched_path = run_dir / "model.enriched.yaml"
    if enriched_path.exists() and (run_dir / "synonyms.json").exists():
        return {"stage": "scenarios", "model_path": enriched_path, "scenarios_path": None}

    if model_path:
        return {"stage": "enrichment", "model_path": model_path, "scenarios_path": None}

    raise ValueError(f"no checkpoint artifacts found in {run_dir}")


def _load_scenarios(path: "pathlib.Path") -> "tuple[list, list]":
    import json
    data = json.loads(path.read_text(encoding="utf-8"))
    golden_set = data.get("golden_set", [])
    questions = data.get("questions", [])
    return golden_set, questions


def _silence_third_party_loggers() -> None:
    """Silence all third-party and internal loggers after they have been imported."""
    _blocked_prefixes = (
        "trulens", "alembic", "snowflake.connector", "snowflake.snowpark",
        "weaver.discovery", "weaver.writer", "weaver.query_history",
        "weaver.enricher", "weaver.scenarios", "weaver.probe",
        "weaver.evaluator", "weaver.refiner",
    )
    for name in list(logging.Logger.manager.loggerDict.keys()):
        if any(name.startswith(p) for p in _blocked_prefixes):
            logging.getLogger(name).setLevel(logging.ERROR)


def _show_plan(iterations: int = 3):
    from rich.align import Align
    from rich.panel import Panel
    from rich.table import Table
    from rich.text import Text

    def _arrow_v():
        console.print(Align("↓", "center"), style="dim cyan")

    def _arrow_h(color: str = "cyan") -> Text:
        return Text(" ──► ", style=f"bold {color}")

    def _box(title: str, file_: str, body: str, color: str = "cyan", w: int = 30) -> Panel:
        hdr = Text()
        hdr.append(title + "\n", style=f"bold {color}")
        hdr.append(file_, style="dim")
        return Panel(body, title=hdr, title_align="left", width=w, border_style=color, padding=(0, 1))

    def _hrow(*cells) -> Table:
        tbl = Table.grid()
        for _ in cells:
            tbl.add_column(vertical="middle")
        tbl.add_row(*cells)
        return tbl

    console.print()
    console.print("  [bold white]Pipeline Plan[/bold white]  [dim]·  --show-plan[/dim]")
    console.print()

    W3 = 32
    W2 = 49
    W4 = 26
    WM = 78

    console.print(Align(_hrow(
        _box("SchemaDiscovery", "discovery.py",
             "Reads INFORMATION_SCHEMA via Snowpark.\n"
             "Profiles columns; samples text/boolean.\n"
             "Infers FK candidates by name + type.\n"
             "[dim]→ SchemaProfile[/dim]", "cyan", W3),
        _arrow_h(),
        _box("YAMLWriter", "writer.py",
             "Rule-based (no LLM). Classifies cols\n"
             "into dimensions / measures /\n"
             "time_dimensions. Builds Relationships.\n"
             "[dim]→ SemanticModel[/dim]", "cyan", W3),
        _arrow_h(),
        _box("QueryHistoryMiner", "query_history.py",
             "Mines QUERY_HISTORY (90-day, ≤1000).\n"
             "Extracts column aliases from SQL\n"
             "as business vocabulary terms.\n"
             "[dim]→ QueryTerms[/dim]", "cyan", W3),
    ), "center"))

    _arrow_v()

    console.print(Align(_hrow(
        _box("SynonymEnricher", "enricher.py",
             "Cortex COMPLETE() (mistral-large2) per table.\n"
             "Descriptions + synonyms grounded by QueryTerms.\n"
             "No structural changes to the model.\n"
             "[dim]→ SemanticModel.enriched[/dim]", "cyan", W2),
        _arrow_h(),
        _box("ScenarioGenerator", "scenarios.py",
             "Cortex COMPLETE() · 5 NL questions per table\n"
             "(including joins). Ground-truth SQL executed\n"
             "directly via Snowpark. Failed SQL dropped.\n"
             "[dim]→ golden_set + questions[/dim]", "cyan", W2),
    ), "center"))

    _arrow_v()

    pad = 46
    console.print(
        Align(
            f"┌{'─' * pad}┐\n"
            f"│  [yellow]loop[/yellow] [dim]· 1 of {iterations} iter(s) shown · stops when correctness ≥ 0.65[/dim]  │\n"
            f"└{'─' * pad}┘",
            "center",
        ),
        highlight=False,
    )

    _arrow_v()

    console.print(Align(_hrow(
        _box("CortexAnalystProbe", "probe.py",
             "Fires NL questions at Cortex\n"
             "Analyst REST API with current\n"
             "YAML; executes returned SQL.\n"
             "[dim]→ ProbeResult[/dim]", "yellow", W4),
        _arrow_h("yellow"),
        _box("Evaluator + TruLens", "evaluator.py",
             "OTEL spans → Snowflake event\n"
             "table. Computes answer_relevance\n"
             "+ correctness server-side.\n"
             "[dim]→ feedback_df[/dim]", "yellow", W4),
        _arrow_h("yellow"),
        _box("VerifiedQuery", "__main__.py",
             "Correctness ≥ 0.80 questions\n"
             "promoted to verified_queries\n"
             "to anchor SQL generation.\n"
             "[dim]→ SemanticModel[/dim]", "yellow", W4),
        _arrow_h("yellow"),
        _box("RefinementAgent", "refiner.py",
             "Cortex COMPLETE() patches\n"
             "synonyms + descriptions only.\n"
             "None on convergence (≥ 0.65).\n"
             "[dim]→ patched SemanticModel[/dim]", "yellow", W4),
        Text(" ↺", style="bold yellow"),
    ), "center"))

    _arrow_v()

    console.print(Align(
        _box(
            "manifest/{DATABASE}.{SCHEMA}/{timestamp}/", "__main__.py",
            "model.yaml  ·  model.enriched.yaml  ·  model.iter{N}.yaml  ·  model.final.yaml\n"
            "synonyms.json  ·  scenarios.json     [dim]resumable via  --resume <run_dir>[/dim]",
            "green", WM,
        ),
    "center"))
    console.print()


def _run_pipeline(
    database: str,
    schema: str,
    iterations: int,
    version: str,
    resume_dir: "pathlib.Path | None" = None,
):
    import contextlib
    import datetime as _dt
    import io
    import pathlib

    from rich.live import Live
    from weaver.dsl import SemanticModel

    if resume_dir:
        ckpt = _detect_checkpoint(resume_dir)
        stage = ckpt["stage"]
        run_dir = resume_dir
        resume_ts = resume_dir.name
        timestamp = _dt.datetime.now().strftime("%Y%m%d_%H%M%S")
        version = f"{version}.{resume_ts}.r{timestamp}"
    else:
        stage = "from_scratch"
        ckpt = {}
        timestamp = _dt.datetime.now().strftime("%Y%m%d_%H%M%S")
        version = f"{version}.{timestamp}"
        run_dir = _run_dir(database, schema, timestamp)

    console.print(f"  [dim]database[/dim]   [bold]{database}[/bold]  [dim]·[/dim]  [dim]schema[/dim]  [bold]{schema}[/bold]")
    console.print(f"  [dim]version[/dim]    [bold]{version}[/bold]")
    console.print(f"  [dim]manifest[/dim]   [dim]{run_dir}[/dim]")
    if resume_dir:
        _info(f"resuming from [bold]{stage}[/bold] stage  ·  model: [bold]{ckpt['model_path'].name}[/bold]")
    console.print()

    session = _session(with_database=True)

    # TruLens init before entering Live so stdout/stderr redirect is safe
    from weaver.evaluator import (
        CortexAnalystApp,
        build_metrics,
        build_session,
        build_tru_app,
        get_results,
        run_evaluation,
    )
    _silence = io.StringIO()
    with contextlib.redirect_stdout(_silence), contextlib.redirect_stderr(_silence):
        tru_session, connector = build_session(session)
    _silence_third_party_loggers()

    # Build the full step list upfront — all steps visible from the first frame
    pre_steps: list[tuple[str, str]] = []
    if stage != "evaluation":
        pre_steps.append(("discovery", "Schema Discovery"))
    pre_steps.append(("writer", "YAML Writer"))
    if stage in ("from_scratch", "enrichment"):
        pre_steps += [("history", "Query History"), ("enrich", "Synonym Enrichment")]
    pre_steps.append(("scenarios", "Scenario Generation"))

    iter_steps: list[tuple[str, str]] = []
    for i in range(1, iterations + 1):
        iter_steps.append((f"probe_{i}", f"Probe + Evaluation    iter {i}"))
        iter_steps.append((f"refine_{i}", f"Refinement Agent      iter {i}"))

    display = _PipelineDisplay(pre_steps + iter_steps)

    with Live(display, console=console, refresh_per_second=8, vertical_overflow="visible"):

        schema_profile = None
        if stage != "evaluation":
            display.start("discovery")
            from weaver.discovery import SchemaDiscovery
            schema_profile = SchemaDiscovery(session).run(database, schema)
            n_fk = sum(len(t["fk_candidates"]) for t in schema_profile["tables"])
            display.complete("discovery", f"{len(schema_profile['tables'])} tables · {n_fk} FK candidates")

        display.start("writer")
        if stage == "from_scratch":
            from weaver.writer import YAMLWriter
            semantic_model = YAMLWriter(session).generate(schema_profile)
            _dump_yaml(run_dir, semantic_model)
            display.complete("writer", f"{len(semantic_model.tables)} tables · {len(semantic_model.relationships)} relationships")
        else:
            semantic_model = SemanticModel.from_yaml_file(str(ckpt["model_path"]))
            display.complete("writer", f"{len(semantic_model.tables)} tables from checkpoint")

        if stage in ("from_scratch", "enrichment"):
            display.start("history")
            from weaver.query_history import QueryHistoryMiner
            query_terms = QueryHistoryMiner(session).mine(database, schema)
            mined_count = sum(len(cols) for cols in query_terms.values())
            if mined_count:
                display.complete("history", f"{mined_count} terms across {len(query_terms)} tables")
            else:
                display.complete("history", "no history found — using column names only")

            display.start("enrich")
            from weaver.enricher import SynonymEnricher
            semantic_model = SynonymEnricher(session).enrich(semantic_model, query_terms=query_terms)
            enriched_path = _dump_yaml(run_dir, semantic_model, suffix=".enriched")
            _dump_synonyms(run_dir, semantic_model)
            display.complete("enrich", f"{len(semantic_model.tables)} tables enriched")

        display.start("scenarios")
        if stage in ("from_scratch", "enrichment", "scenarios"):
            from weaver.scenarios import ScenarioGenerator
            golden_set, questions = ScenarioGenerator(session).generate(schema_profile)
            _dump_scenarios(run_dir, golden_set, questions)
            display.complete("scenarios", f"{len(questions)} scenarios")
        else:
            golden_set, questions = _load_scenarios(ckpt["scenarios_path"])
            display.complete("scenarios", f"{len(questions)} from checkpoint")

        metrics = build_metrics(session, golden_set)

        for iteration in range(1, iterations + 1):
            iter_version = f"{version}.iter{iteration}"
            pk, rk = f"probe_{iteration}", f"refine_{iteration}"

            from weaver.probe import CortexAnalystProbe
            probe = CortexAnalystProbe(session, semantic_model.to_yaml())
            app = CortexAnalystApp(probe)
            tru_app = build_tru_app(app, connector=connector, version=iter_version)

            display.start(pk)
            run_evaluation(tru_app, app, questions, metrics, version=iter_version)
            records_df, feedback_df = get_results(tru_session, snowpark_session=session, version=iter_version)

            mean_correctness = feedback_df["correctness"].mean() if "correctness" in feedback_df.columns else 0.0
            mean_relevance = feedback_df["answer_relevance"].mean() if "answer_relevance" in feedback_df.columns else 0.0
            semantic_model, promoted = _promote_verified_queries(
                semantic_model, feedback_df, app.results, iter_version
            )
            probe_result = (
                f"{len(records_df)} scored · correctness {mean_correctness:.2f} · relevance {mean_relevance:.2f}"
                + (f" · {promoted} verified" if promoted else "")
            )
            display.complete(pk, probe_result)

            display.start(rk)
            from weaver.refiner import RefinementAgent
            patch = RefinementAgent(session).refine(semantic_model, feedback_df)
            if patch is None:
                display.complete(rk, "converged")
                for j in range(iteration + 1, iterations + 1):
                    display.complete(f"probe_{j}", "—")
                    display.complete(f"refine_{j}", "—")
                break
            semantic_model = patch
            refined_path = _dump_yaml(run_dir, semantic_model, suffix=f".iter{iteration}")
            _dump_synonyms(run_dir, semantic_model, suffix=f".iter{iteration}")
            display.complete(rk, f"patched  →  {refined_path}")

    final_path = _dump_yaml(run_dir, semantic_model, suffix=".final")
    console.print()
    console.rule(style="cyan dim")
    _ok(f"final model  →  [bold]{final_path}[/bold]")
    _info("view results in [bold]Snowsight → AI & ML → Evaluations[/bold]")
    console.print()


def main():
    load_dotenv()

    # Rich console is the UI — block all pipeline and third-party log output.
    # A filter on the root handler catches every propagating child logger,
    # regardless of when the logger is created (covers dynamic TruLens loggers).
    _blocked_prefixes = (
        "trulens", "alembic",
        "snowflake.connector", "snowflake.snowpark",
        "weaver.discovery", "weaver.writer", "weaver.query_history",
        "weaver.enricher", "weaver.scenarios", "weaver.probe",
        "weaver.evaluator", "weaver.refiner",
    )

    class _PipelineFilter(logging.Filter):
        def filter(self, record: logging.LogRecord) -> bool:
            return not any(record.name.startswith(p) for p in _blocked_prefixes)

    _handler = logging.StreamHandler()
    _handler.addFilter(_PipelineFilter())
    _handler.setFormatter(logging.Formatter("%(levelname)s %(name)s %(message)s"))
    logging.root.setLevel(logging.WARNING)
    logging.root.handlers.clear()
    logging.root.addHandler(_handler)

    import argparse

    _print_banner()

    parser = argparse.ArgumentParser(
        prog="weaver",
        description="Semantic Model Weaver — Cortex Analyst semantic model generator and evaluator.",
    )
    parser.add_argument("--show-plan", action="store_true", help="Print the pipeline execution plan and exit.")
    parser.add_argument("--setup", action="store_true", help="Create workspace DB, schema, and network policy. Safe to re-run.")
    parser.add_argument("--reset-workspace", action="store_true", help="Drop and recreate the TruLens schema. Clears all evaluation records.")
    parser.add_argument("--yes", action="store_true", help="Skip confirmation prompt for --reset-workspace.")
    parser.add_argument("--database", help="Snowflake database to weave a model for.")
    parser.add_argument("--schema", help="Snowflake schema to weave a model for.")
    parser.add_argument("--iterations", type=int, default=3, help="Max refinement iterations (default: 3).")
    parser.add_argument("--version", default="v1", help="TruLens app version tag (default: v1).")
    parser.add_argument(
        "--resume",
        metavar="RUN_DIR",
        help=(
            "Resume from a previous run directory, e.g. "
            "manifest/NEXTRADE_EQUITY_MARKET_DATA.FIN/20260405_232125/. "
            "The stage is detected automatically from the artifacts present: "
            "scenarios.json → evaluation; synonyms.json → scenarios; model.yaml → enrichment. "
            "--database and --schema are inferred from the path if not given."
        ),
    )
    args = parser.parse_args()

    if args.show_plan:
        _show_plan(iterations=args.iterations)
        return

    if args.setup:
        _setup()
        return

    if args.reset_workspace:
        _reset_workspace(yes=args.yes)
        return

    import pathlib

    resume_dir: pathlib.Path | None = None
    if args.resume:
        resume_dir = pathlib.Path(args.resume).resolve()
        if not resume_dir.is_dir():
            parser.error(f"--resume path does not exist or is not a directory: {resume_dir}")
        # Infer database/schema from path (manifest/{DATABASE}.{SCHEMA}/{timestamp}/)
        if not args.database or not args.schema:
            db_schema_part = resume_dir.parent.name
            if "." in db_schema_part:
                inferred_db, inferred_schema = db_schema_part.split(".", 1)
                args.database = args.database or inferred_db
                args.schema = args.schema or inferred_schema
            else:
                parser.error("could not infer --database/--schema from resume path; provide them explicitly")

    if not args.resume and (not args.database or not args.schema):
        parser.error("--database and --schema are required (or use --resume / --setup / --reset-workspace)")

    _run_pipeline(args.database, args.schema, args.iterations, args.version, resume_dir=resume_dir)


if __name__ == "__main__":
    main()
