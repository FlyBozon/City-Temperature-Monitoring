FROM python:3.11-slim

# Install dependencies for confluent-kafka (librdkafka is bundled, but SSL/certs helpful)
RUN apt-get update && apt-get install -y --no-install-recommends         ca-certificates         && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

COPY producer.py /app/

CMD ["python", "-u", "producer.py"]
