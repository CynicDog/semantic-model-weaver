FROM python:3.13-slim

WORKDIR /app

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

COPY pyproject.toml uv.lock .python-version ./
RUN uv sync --no-dev --frozen

COPY weaver/ ./weaver/

ENTRYPOINT ["uv", "run", "python", "-m", "weaver"]
