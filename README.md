# clickstrem-event-processor
This is a fun little side project to simulate clickstrem data for simulating realtime ingestion data processing

## Kafka clickstream producer

This workspace includes a small Python producer that generates random e-commerce clickstream events and publishes them to a Kafka topic, plus a simple consumer for verification.

Files added:

- `producer.py`: generates random clickstream events and sends them to Kafka.
- `consumer.py`: simple console consumer to read events back from Kafka.
- `docker-compose.yml`: spins up Zookeeper + Kafka for local testing.
- `requirements.txt`: Python dependencies.

Quick start (local):

1. Start Kafka locally with Docker Compose:

```bash
docker-compose up -d
```

2. Install Python dependencies:

```bash
python -m pip install -r requirements.txt
```

3. Run the producer (default: localhost:9092, topic `clickstream`):

```bash
python producer.py --rate 10
```

4. (Optional) Run the consumer to verify messages:

```bash
python consumer.py
```

Environment variables:

- `KAFKA_BOOTSTRAP`: Kafka bootstrap server (default `localhost:9092`).
- `KAFKA_TOPIC`: Topic name (default `clickstream`).

See the individual scripts for more CLI options.

## Tests

Quick test instructions for this project:

- **Install test dependencies:**

	```bash
	python -m pip install -r requirements.txt
	```

- **Run the test suite:**

	```bash
	python -m pytest -q
	```

- **Notes:**
	- Tests live in the `tests/` directory. `tests/conftest.py` adds the project root to `sys.path` so tests can import the top-level modules.

## Flink analytics (basic)

This repository includes `flink_job.py`, a small PyFlink streaming job that:

- Reads JSON events from Kafka topic `clickstream`.
- Computes a 1-minute tumbling window count grouped by `event_type`.
- Writes aggregated results to a `print` sink (console).

Quick run (local, requires `pyflink`):

```bash
python -m pip install -r requirements.txt
python flink_job.py --bootstrap localhost:9092 --topic clickstream
```

Alternative: submit to a Flink cluster (recommended for production):

1. Start a Flink cluster (for example using the official Flink Docker image).
2. Submit the job:

```bash
./bin/flink run -py flink_job.py --bootstrap kafka:9092 --topic clickstream
```

Notes:
- The job uses processing-time windows (`PROCTIME()`), so it doesn't depend on event timestamps.
- If you prefer event-time windows, modify the source schema to parse `ts` into a TIMESTAMP and add watermarks.

