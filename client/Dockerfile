FROM python:3.13-slim-bullseye

ARG OPENFGA_CLI_VERSION=0.7.8

WORKDIR /code

RUN apt-get update -y
RUN apt-get install -y --no-install-recommends ca-certificates curl unzip

# Install OpenFGA CLI
RUN set -eux; \
    arch="$(dpkg --print-architecture)"; \
    case "$arch" in \
    amd64)  oarch="amd64" ;; \
    arm64)  oarch="arm64"  ;; \
    *) echo "Unsupported arch: $arch" && exit 1 ;; \
    esac; \
    curl -fsSL -o /tmp/openfga.tar.gz \
    "https://github.com/openfga/cli/releases/download/v${OPENFGA_CLI_VERSION}/fga_${OPENFGA_CLI_VERSION}_linux_${oarch}.tar.gz"; \
    tar -xzf /tmp/openfga.tar.gz -C /usr/local/bin fga; \
    rm /tmp/openfga.tar.gz; \
    /usr/local/bin/fga version

COPY --from=ghcr.io/astral-sh/uv:0.8.13 /uv /bin/uv

COPY uv.lock pyproject.toml .python-version ./
RUN uv sync --frozen --no-cache --no-dev

ENV PATH="/code/.venv/bin:$PATH"
ENV PYTHONPATH=/code

COPY . .
