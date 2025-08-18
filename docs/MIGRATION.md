# Migration from HTTP to Kafka

1. Parallel run: keep HTTP calls and publish equivalent Kafka events.
2. Migrate consumers to process from Kafka.
3. Cut over: remove HTTP coupling once stable.
4. Fallback: if Kafka unavailable, buffer via outbox and retry.
