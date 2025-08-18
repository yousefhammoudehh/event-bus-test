# Local Kafka Bootstrap

- Use `confluentinc/cp-kafka` or `bitnami/kafka` docker-compose for local dev.
- Set `KAFKA_BOOTSTRAP_SERVERS=localhost:9092` in your environment.
- Run `scripts/create_topics.py` to create initial topics.
