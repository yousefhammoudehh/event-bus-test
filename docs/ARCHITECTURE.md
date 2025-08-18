# Olive Events Bus Architecture

This document outlines the event-driven architecture for Olive using Kafka and the internal SDK.

- Event producers publish to Kafka topics: service.entity.event.version
- JSON schemas enforce payload structure and compatibility.
- Consumers validate, process, retry with backoff, and send failures to DLQ.
- Observability: structured logs, correlation IDs, and optional tracing/metrics.

High-level flow:

```text
 [Service A Producer]
    |
    v
   [Kafka Topic: service.entity.event.v1] ---> [DLQ Topic: service.entity.event.v1.dlq]
    |
    v
 [Service B Consumer Group]
```
