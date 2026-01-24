# Use the official Python image as a parent image
ARG BASE_IMAGE=python:3.13-slim

FROM ${BASE_IMAGE} as builder
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

# Install dependencies
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --locked --no-install-project --group dev

FROM ${BASE_IMAGE} as runtime

WORKDIR /app

# Copy the environment
COPY --from=builder /app/.venv /app/.venv

# Add the virtual environment to PATH
ENV PATH="/app/.venv/bin:$PATH"

# Copy the project files
COPY  . .
