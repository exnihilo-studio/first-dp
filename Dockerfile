FROM python:3.13-slim@sha256:739e7213785e88c0f702dcdc12c0973afcbd606dbf021a589cab77d6b00b579d AS builder

WORKDIR /app

COPY --from=ghcr.io/astral-sh/uv:0.9.26 /uv /usr/local/bin/uv

COPY pyproject.toml uv.lock README.md ./

COPY airflow/src/multiply_by_23.py ./multiply_by_23.py

RUN uv sync --frozen --no-dev


FROM python:3.13-slim@sha256:739e7213785e88c0f702dcdc12c0973afcbd606dbf021a589cab77d6b00b579d

WORKDIR /app

COPY --from=builder /app /app

ENV PATH="/app/.venv/bin:$PATH"

RUN useradd --create-home --shell /bin/bash app
USER app

