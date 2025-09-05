# City Temperature Monitoring (Docker + Kafka + Spark)

Prosty pipeline: Python producer -> Kafka -> Spark Structured Streaming -> CSV/Parquet.

## Wymagania
- Docker + Docker Compose

## Szybki start
```bash
docker compose up -d --build
```

### Co się uruchamia?
- **Zookeeper** i **Kafka** (Confluent 7.6.x)
- **Spark** (Bitnami Spark 3.5.x) z jobem streamingowym
- **Producer** (Python 3.11 + confluent-kafka) generujący dane

### Gdzie są wyniki?
- `./shared/output/anomalies_out_of_range_csv/` (CSV z nagłówkiem)
- `./shared/output/anomalies_volatility_parquet/` (Parquet)
- Dodatkowo, anomalie OUT_OF_RANGE lecą na konsolę kontenera Spark.

## Podgląd logów
```bash
docker compose logs -f producer
docker compose logs -f spark
docker compose logs -f kafka
```

## Zatrzymanie
```bash
docker compose down
```

## Zmiana progu anomalii
W pliku `spark_app/spark_streaming.py`:
- OUT_OF_RANGE: temperatury < -30 lub > 50
- VOLATILITY (okno 2 min, slide 30 s): spread > 10°C
