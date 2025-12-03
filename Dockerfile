FROM ghcr.io/astral-sh/uv:0.9.15@sha256:4c1ad814fe658851f50ff95ecd6948673fffddb0d7994bdb019dcb58227abd52 AS uv


FROM python:3.14.0@sha256:edf6433343f65f94707985869aeaafe8beadaeaee11c4bc02068fca52dce28dd AS builder

RUN apt-get update && \
    apt-get install -y git && \
    rm -rf /var/lib/apt/lists/*

COPY --from=uv /uv /uvx /bin/

WORKDIR /usr/src/app

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never

COPY pyproject.toml uv.lock ./

RUN uv sync --frozen --no-dev --extra app --no-install-project

COPY . .

RUN uv sync --frozen --no-dev --extra app



FROM python:3.14.0-slim@sha256:0aecac02dc3d4c5dbb024b753af084cafe41f5416e02193f1ce345d671ec966e

WORKDIR /usr/src/app

RUN apt-get update && \
    apt-get install -y ffmpeg && \
    rm -rf /var/lib/apt/lists/*

ENV PATH="/usr/src/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

COPY --from=builder /usr/src/app .

EXPOSE 9080

CMD [ "python", "-m", "granian", "--interface", "asginl", "src.main:app", "--host", "0.0.0.0", "--port", "9080" ]
