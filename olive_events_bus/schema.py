import asyncio
import json
import os
import re
from pathlib import Path
from typing import Any

import aiofiles
from jsonschema import Draft202012Validator


class SchemaRegistry:
    """Simple file-based JSON schema registry embedded in the SDK.

    Layout under schema_dir:
      - <domain>/<entity>.<event>.<version>.json
    Example:
      - leaves/leave.requested.v1.json
      - user/user.created.v1.json
    """

    def __init__(self, schema_dir: str):
        self.base = Path(schema_dir)
        self._cache: dict[str, dict[str, Any]] = {}
        self._validators: dict[str, Draft202012Validator] = {}
        # Cache of latest version per (domain, entity, event)
        self._latest_cache: dict[str, str] = {}

    async def load(self, domain: str, entity: str, event: str, version: str) -> dict[str, Any]:
        key = self._key(domain, entity, event, version)
        if key not in self._cache:
            path = self._schema_path(domain, entity, event, version)
            # Use aiofiles to read without blocking the event loop
            async with aiofiles.open(path, encoding='utf-8') as f:
                text = await f.read()
            self._cache[key] = json.loads(text)
        return self._cache[key]

    async def validator(self, domain: str, entity: str, event: str, version: str) -> Draft202012Validator:
        key = self._key(domain, entity, event, version)
        if key not in self._validators:
            schema = await self.load(domain, entity, event, version)
            self._validators[key] = Draft202012Validator(schema)
        return self._validators[key]

    async def validate(self, domain: str, entity: str, event: str, version: str, payload: dict[str, Any]) -> None:
        validator = await self.validator(domain, entity, event, version)
        # Validation can be CPU-bound for large schemas; offload to a thread to avoid blocking
        await asyncio.to_thread(validator.validate, payload)

    def topic(self, domain: str, entity: str, event: str) -> str:
        """Return Kafka topic name using: <domain>.<entity>.<event>"""
        return f'{domain}.{entity}.{event}'

    async def latest_version(self, domain: str, entity: str, event: str) -> str:
        """Discover and cache the latest schema version for a given (domain, entity, event).

        Versions are expected to be of the form 'v<N>' where <N> is an integer. The
        highest numeric version is considered the latest.
        """
        lkey = self._latest_key(domain, entity, event)
        if lkey in self._latest_cache:
            return self._latest_cache[lkey]

        dir_path = self.base / domain
        if not dir_path.exists():
            raise FileNotFoundError(f'Schema domain not found: {dir_path}')

        prefix = f'{entity}.{event}.'
        versions: list[tuple[int, str]] = []
        ver_re = re.compile(r'^v(\d+)$')
        # Collect matching files off the event loop
        paths = await asyncio.to_thread(lambda: list(dir_path.glob(f'{entity}.{event}.*.json')))
        for p in paths:
            name = p.name  # e.g., user.created.v1.json
            if not name.startswith(prefix) or not name.endswith('.json'):
                continue
            ver_part = name[len(prefix) : -len('.json')]  # e.g., v1
            m = ver_re.match(ver_part)
            if not m:
                continue
            versions.append((int(m.group(1)), ver_part))

        if not versions:
            raise FileNotFoundError(f'No schema versions found for {domain}/{entity}.{event} under {dir_path}')

        # Pick the highest numeric version
        _, latest = max(versions, key=lambda t: t[0])
        self._latest_cache[lkey] = latest
        return latest

    def _key(self, domain: str, entity: str, event: str, version: str) -> str:
        return f'{domain}:{entity}.{event}:{version}'

    def _latest_key(self, domain: str, entity: str, event: str) -> str:
        return f'{domain}:{entity}.{event}'

    def _schema_path(self, domain: str, entity: str, event: str, version: str) -> Path:
        return self.base / domain / f'{entity}.{event}.{version}.json'


# Module-level singleton for convenience
def _default_schema_dir() -> str:
    """Resolve default schema directory.

    Priority:
        1) EVENT_SCHEMA_DIR env var (if set)
        2) bundled schemas at <package>/schemas
    """
    env = os.getenv('EVENT_SCHEMA_DIR')
    if env:
        return env
    return str(Path(__file__).resolve().parent / 'schemas')


# Ready-to-use singleton instance
schema_registry = SchemaRegistry(schema_dir=_default_schema_dir())
