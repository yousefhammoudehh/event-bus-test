# Schema Governance

- JSON schemas stored under `olive_events_bus/schemas/` and versioned (e.g., `user.created.v1.json`).
- Backward compatible changes only within same major version.
- Breaking change => bump version and create new topic suffix.
- Validate on both produce and consume paths.
