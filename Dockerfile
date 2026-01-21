# Persona Platform - Test Data Generation
# Multi-stage build for smaller final image

FROM python:3.12-slim AS builder

WORKDIR /app

# Install build dependencies
RUN pip install --no-cache-dir build

# Copy project files
COPY pyproject.toml .
COPY src/ src/

# Build the wheel
RUN python -m build --wheel

# Final stage
FROM python:3.12-slim

LABEL maintainer="Test Data Generation Team"
LABEL description="Persona Platform - Generate test datasets from behavioral personas"
LABEL version="0.1.0"

WORKDIR /app

# Create non-root user for security
RUN useradd --create-home --shell /bin/bash appuser

# Copy built wheel from builder stage
COPY --from=builder /app/dist/*.whl /tmp/

# Install the package
RUN pip install --no-cache-dir /tmp/*.whl && rm /tmp/*.whl

# Copy examples for reference
COPY examples/ /app/examples/

# Create output directory
RUN mkdir -p /app/output && chown -R appuser:appuser /app

# Switch to non-root user
USER appuser

# Set default output directory
ENV PERSONA_OUTPUT_DIR=/app/output

# Expose volume for output
VOLUME ["/app/output", "/app/personas", "/app/profiles"]

# Default command shows help
ENTRYPOINT ["persona-gen"]
CMD ["--help"]
