from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator


class SchemaRegistry:
    """Simple file-based JSON schema registry embedded in the SDK.

    Layout under schema_dir:
      - <domain>/<name>.<version>.json
    Example:
      - user/user.created.v1.json
      - leaves/leave.requested.v1.json
    """

    def __init__(self, schema_dir: str):
        self.base = Path(schema_dir)
        self._cache: dict[str, dict[str, Any]] = {}
        self._validators: dict[str, Draft202012Validator] = {}

    def _key(self, domain: str, schema_name: str, version: str) -> str:
        return f'{domain}:{schema_name}:{version}'

    def load(self, domain: str, schema_name: str, version: str) -> dict[str, Any]:
        key = self._key(domain, schema_name, version)
        if key not in self._cache:
            path = self.base / domain / f'{schema_name}.{version}.json'
            with path.open('r', encoding='utf-8') as f:
                self._cache[key] = json.load(f)
        return self._cache[key]

    def validator(self, domain: str, schema_name: str, version: str) -> Draft202012Validator:
        key = self._key(domain, schema_name, version)
        if key not in self._validators:
            schema = self.load(domain, schema_name, version)
            self._validators[key] = Draft202012Validator(schema)
        return self._validators[key]

    def validate(self, domain: str, schema_name: str, version: str, payload: dict[str, Any]) -> None:
        validator = self.validator(domain, schema_name, version)
        validator.validate(payload)

    def topic(self, service: str, entity: str, event: str, version: str) -> str:
        return f'{service}.{entity}.{event}.{version}'
