.PHONY: start stop produce consume logs clean

TOPIC = order-events
RATE  = 5

# ─────────────────────────────────────────────────────────────────────────────

## Start Kafka broker + Kafka UI
start:
	docker compose up -d
	@echo ""
	@echo "  Kafka broker : localhost:9092"
	@echo "  Kafka UI     : http://localhost:8080"
	@echo ""
	@echo "  Wait ~10s for the health check, then run 'make produce' in one"
	@echo "  terminal and 'make consume' in another."

## Stop and remove containers (data volume is preserved)
stop:
	docker compose down

## Publish order events to Kafka  (default: 5 events/sec)
## Override rate:  make produce RATE=20
produce:
	cd producer && pip install -q -r requirements.txt && \
	python order_producer.py --topic $(TOPIC) --rate $(RATE)

## Start the Spark Structured Streaming job
## Packages are resolved on first run (~2 min); cached on subsequent runs
consume:
	spark-submit \
		--packages io.delta:delta-spark_2.12:3.0.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
		spark/streaming_consumer.py

## Tail Kafka broker logs
logs:
	docker compose logs -f kafka

## Delete local Delta tables and checkpoints (fresh-start the streaming job)
clean:
	rm -rf delta/ checkpoints/
	@echo "Removed ./delta and ./checkpoints"
