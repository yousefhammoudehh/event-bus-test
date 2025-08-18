# Schema Registry

- File-based registry embedded in SDK under `olive_events_bus/schemas`.
- Naming: domain/name.version.json (e.g., `user/user.created.v1.json`).
- Validate on produce and consume.
- Backward-compatibility rules enforced via versioning and reviews.
