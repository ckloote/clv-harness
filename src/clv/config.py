"""Harness parameters from config/params.toml (DESIGN.md §10).

No threshold is defined anywhere else: code asks for a §10 key by name, and an
unknown key is an error rather than a silent default.

    from clv.config import param
    buffer_s = param("close.buffer_s")
"""
from __future__ import annotations

import tomllib
from dataclasses import dataclass, field
from functools import cache
from pathlib import Path
from typing import Any

PARAMS_PATH = Path(__file__).resolve().parents[2] / "config" / "params.toml"
STATUSES = ("fixed", "provisional")


@dataclass(frozen=True)
class Param:
    key: str
    value: Any
    status: str
    revised_by: str
    extra: dict[str, Any] = field(default_factory=dict)   # fields §10 states in prose


def _flatten(table: dict, prefix: str = "") -> dict[str, Param]:
    """A table with a `value` field is a parameter; any other table is a section."""
    out = {}
    for name, body in table.items():
        key = f"{prefix}{name}"
        if not isinstance(body, dict):
            raise ValueError(f"{key}: expected a table, got {type(body).__name__}")
        if "value" in body:
            missing = {"status", "revised_by"} - body.keys()
            if missing:
                raise ValueError(f"{key}: missing {sorted(missing)}")
            if body["status"] not in STATUSES:
                raise ValueError(f"{key}: status {body['status']!r} not in {STATUSES}")
            if not str(body["revised_by"]).strip():
                raise ValueError(f"{key}: empty revised_by")
            extra = {k: v for k, v in body.items() if k not in ("value", "status", "revised_by")}
            out[key] = Param(key, body["value"], body["status"], body["revised_by"], extra)
        else:
            out.update(_flatten(body, f"{key}."))
    return out


def load_params(path: Path = PARAMS_PATH) -> dict[str, Param]:
    with open(path, "rb") as f:
        return _flatten(tomllib.load(f))


@cache
def _default() -> dict[str, Param]:
    return load_params()


def param(key: str) -> Any:
    """The value of a §10 key. KeyError for a key not in params.toml."""
    return _get(key).value


def param_field(key: str, name: str) -> Any:
    """A field §10 states in prose beside a key's value, e.g. `requires_settlement_equivalence`."""
    return _get(key).extra[name]


def _get(key: str) -> Param:
    try:
        return _default()[key]
    except KeyError:
        raise KeyError(f"{key!r} is not in {PARAMS_PATH.name}; add it to DESIGN.md §10 first") from None
