FROM node:24-alpine AS frontend-builder

WORKDIR /build/frontend
COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci
COPY frontend/ ./
RUN npm run build


FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim AS runtime

ARG APP_UID=1000
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    HOME=/home/app \
    LOCAL_FINANCE_DATA_DIR=/data \
    LOCAL_FINANCE_FRONTEND_DIST=/app/frontend/dist

WORKDIR /app
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-dev --no-install-project

COPY app.py ./
COPY src/ ./src/
RUN uv sync --frozen --no-dev

RUN useradd --uid "${APP_UID}" --create-home app \
    && mkdir -p /data \
    && chown app:app /data
COPY --from=frontend-builder --chown=app:app /build/frontend/dist ./frontend/dist

USER app
EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
    CMD ["/app/.venv/bin/python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/api/health', timeout=3)"]

CMD ["/app/.venv/bin/uvicorn", "local_finance.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1", "--proxy-headers", "--forwarded-allow-ips", "*"]
