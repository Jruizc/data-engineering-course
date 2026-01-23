FROM python:3.13.11-slim

# Copy uv binary from official uv image (multi-stage build pattern)
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

WORKDIR /code

# Copy dependency files first (better layer caching)
COPY "pyproject.toml" "uv.lock" ".python-version" ./

# Add virtual environment to PATH so we can use installed packages
ENV PATH="/code/.venv/bin:$PATH"

# Install dependencies from lock file (ensures reproducible builds)
RUN uv sync --locked

RUN pip install pandas pyarrow



COPY pipeline.py .


ENTRYPOINT [ "python", "pipeline.py" ]