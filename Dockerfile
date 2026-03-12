# Dockerfile
# ═══════════════════════════════════════════════════════════
# PURPOSE: Containerise the retail ETL pipeline
#
# WHY DOCKER?
# "Works on my machine" is not good enough for production
# Docker packages your code + Python + dependencies
# into one portable container that runs identically
# on your laptop, a server, or in the cloud ✅
# ═══════════════════════════════════════════════════════════

# ── Base image ────────────────────────────────────────────
# python:3.11-slim = Python 3.11 on minimal Linux
# slim = smaller image, no unnecessary packages
# WHY not full python image?
# Full image = 900MB, slim = 130MB
# Smaller = faster to build, faster to deploy ✅
FROM python:3.11-slim

# ── Set working directory ─────────────────────────────────
# All commands below run from /app
# All files are copied into /app
WORKDIR /app

# ── Install system dependencies ───────────────────────────
# libpq-dev  → needed for psycopg2 (PostgreSQL driver)
# gcc        → needed to compile psycopg2
# Without these → pip install psycopg2 fails ❌
RUN apt-get update && apt-get install -y \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# ── Copy requirements first ───────────────────────────────
# WHY copy requirements before code?
# Docker caches each layer
# If requirements.txt hasn't changed → skip pip install
# Only re-installs when requirements change ✅
# This is the most important Docker optimisation
COPY requirements.txt .

# ── Install Python dependencies ───────────────────────────
# --no-cache-dir → don't store pip cache in image
# Keeps image smaller ✅
RUN pip install --no-cache-dir -r requirements.txt

# ── Copy project code ─────────────────────────────────────
# Copy everything else after dependencies
# Code changes → only this layer rebuilds
# Dependencies don't reinstall ✅
COPY . .

# ── Create logs directory ─────────────────────────────────
RUN mkdir -p logs

# ── Default command ───────────────────────────────────────
# What runs when you do: docker run retail-etl
# Can be overridden at runtime
CMD ["python", "run_pipeline.py"]