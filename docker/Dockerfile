FROM python:3.10-slim

# Set a working directory
WORKDIR /app

# Install any global dependencies (if needed)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
  && rm -rf /var/lib/apt/lists/*

# Upgrade pip
RUN pip install --upgrade pip

CMD ["python"]
