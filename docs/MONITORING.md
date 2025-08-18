# Monitoring and Alerting

Metrics (Prometheus):

- olive_events_published_total{topic,service,version}
- olive_events_consumed_total{topic,service,group}
- olive_events_failed_total{topic,service,group}

Alert suggestions:

- High consumer lag (from Kafka Exporter): > N behind for M minutes.
- Growth in olive_events_failed_total rate.
- Retry storms detected via logs.
