# ContentFlow API - Dockerfile for Azure Container Apps deployment
FROM python:3.13-slim

# Set working directory
WORKDIR /app

# Install system dependencies (including Playwright/Chromium deps)
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    # Playwright Chromium dependencies
    libnss3 \
    libnspr4 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libdrm2 \
    libdbus-1-3 \
    libxkbcommon0 \
    libatspi2.0-0 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxrandr2 \
    libgbm1 \
    libpango-1.0-0 \
    libcairo2 \
    libasound2 \
    libwayland-client0 \
    && rm -rf /var/lib/apt/lists/*

# Copy contentflow-lib first (shared dependency)
COPY contentflow-lib /app/contentflow-lib

# Install contentflow-lib
WORKDIR /app/contentflow-lib
RUN pip install --no-cache-dir -e .

# Switch to API directory
WORKDIR /app/contentflow-api

# Copy API requirements
COPY contentflow-api/requirements.txt .

# Install API dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Install Playwright and Chromium browser
RUN pip install --no-cache-dir playwright && playwright install chromium

# Copy API code
COPY contentflow-api/ .

# Environment variables (can be overridden at runtime)
ENV PYTHONUNBUFFERED=1
ENV API_SERVER_HOST=0.0.0.0
ENV API_SERVER_PORT=8090

# Expose port
EXPOSE 8090

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD python -c "import requests; requests.get('http://localhost:8090/health')" || exit 1

# Run the API server
CMD ["python", "main.py"]
