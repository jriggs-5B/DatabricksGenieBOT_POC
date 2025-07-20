# Dockerfile

# 1. Use the slim Python 3.10 base image
FROM python:3.10-slim

# 2. Create & switch to the app directory
WORKDIR /app

# 3. Copy requirements and install them
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
 && pip install --no-cache-dir -r requirements.txt


# 4. Copy the rest of your application code
COPY . .

# 5. Tell Docker (and any tools) that we listen on 8000 by default
ENV PORT=8181
EXPOSE 8181

# Run via shell so $PORT expands
CMD \
  gunicorn \
    --bind=0.0.0.0:$PORT \
    --worker-class aiohttp.worker.GunicornWebWorker \
    --timeout 1200 \
    app:app
