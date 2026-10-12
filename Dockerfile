FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PYTHONPATH=/app/src
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src ./src
RUN useradd --create-home --uid 10001 app && mkdir -p /data/bronze && chown -R app /app /data/bronze
USER app

EXPOSE 8000
# One worker: the rate limiter lives in memory. --proxy-headers so the visitor IP comes from Caddy;
# trusting any forwarder is safe only because port 8000 is never published (see docker-compose.yml).
CMD ["uvicorn", "smartingest.api:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1", \
     "--proxy-headers", "--forwarded-allow-ips", "*", "--no-server-header"]