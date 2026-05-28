# ContentFlow Worker - Multi-processing Content Processing Engine
FROM python:3.13-slim

# Set working directory
WORKDIR /app

# Install system dependencies (including Playwright/Chromium deps)
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    wget \
    gnupg \
    libnss3 \
    libnspr4 \
    libdbus-1-3 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libdrm2 \
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
    libx11-xcb1 \
    fonts-liberation \
    && rm -rf /var/lib/apt/lists/*

# Copy contentflow-lib first (shared dependency)
COPY contentflow-lib /app/contentflow-lib

# Install contentflow-lib
WORKDIR /app/contentflow-lib
RUN pip install --no-cache-dir -e .

# Switch to Worker directory
WORKDIR /app/contentflow-worker

# Copy Worker requirements
COPY contentflow-worker/requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Install Playwright and Chromium browser
RUN pip install --no-cache-dir playwright && playwright install chromium

# Copy worker code
COPY contentflow-worker/ .

# # Set Python path to include contentflow-lib
# ENV PYTHONPATH="/app:/app/contentflow-lib:${PYTHONPATH}"

# Environment variables (can be overridden)
ENV WORKER_NAME=contentflow-worker
ENV NUM_PROCESSING_WORKERS=4
ENV NUM_SOURCE_WORKERS=2
ENV LOG_LEVEL=INFO

# Run the worker
CMD ["python", "main.py"]
