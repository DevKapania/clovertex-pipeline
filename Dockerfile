# ── Stage 1: Base image ───────────────────────────────────
# We use Python 3.11 slim — smaller image than full Python
# "slim" = only essential OS packages, no extras
# This makes our container faster to download and more secure
FROM python:3.11-slim

# What is a Docker image?
# A snapshot of an operating system + software.
# Like a USB drive with everything pre-installed.
# Anyone who runs this image gets the exact same environment.

# ── Set working directory inside container ────────────────
# All subsequent commands run from /app
WORKDIR /app

# What is WORKDIR?
# Like doing "cd /app" permanently inside the container.
# All files will be placed here.

# ── Install system dependencies ───────────────────────────
# These are OS-level packages needed by Python libraries
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Why gcc/g++?
# Some Python libraries (like numpy) compile C extensions.
# Without a C compiler they fail to install.
# rm -rf /var/lib/apt/lists/* cleans up cache to reduce image size.

# ── Copy and install Python dependencies ─────────────────
# Copy requirements.txt FIRST (before rest of code)
# Why? Docker caches layers. If requirements.txt doesn't change,
# Docker reuses the cached pip install layer — much faster builds.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# What is --no-cache-dir?
# Tells pip not to cache downloaded packages.
# Saves disk space inside the container.

# ── Copy project code ─────────────────────────────────────
COPY pipeline/ ./pipeline/
COPY data/ ./data/

# ── Create output directories ─────────────────────────────
RUN mkdir -p datalake/raw \
    datalake/refined \
    datalake/consumption/plots

# ── Run the pipeline ──────────────────────────────────────
# CMD is what runs when someone does "docker run"
# python -m pipeline.main runs our master script
CMD ["python", "-m", "pipeline.main"]