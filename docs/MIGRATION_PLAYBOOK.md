# Migration Playbook

Phases:

1. Instrument producers to emit events alongside HTTP calls.
2. Build consumers to process events and keep existing HTTP handlers.
3. Verify parity and idempotency; monitor DLQ.
4. Gradually disable HTTP flows; keep outbox/CDC for durability.
