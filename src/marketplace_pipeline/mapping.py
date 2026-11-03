"""Versioned declarative field mappings, with explicit conversion failures."""

import unicodedata
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation

MISSING = object()


class MappingError(ValueError):
    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


def normalize(value: str) -> str:
    return " ".join(unicodedata.normalize("NFC", value).split())


def path_value(item: dict, path: str):
    value = item
    for part in path.split("."):
        if not isinstance(value, dict) or part not in value:
            return MISSING
        value = value[part]
    return value


def convert(value, kind):
    if kind == "identity":
        return value
    if kind == "text":
        if not isinstance(value, str) or not normalize(value):
            raise ValueError
        return normalize(value)
    if kind == "decimal":
        if isinstance(value, bool) or not isinstance(value, (str, int, Decimal)):
            raise ValueError
        result = Decimal(value)
        if not result.is_finite():
            raise ValueError
        return result
    if kind == "datetime":
        result = datetime.fromisoformat(value) if isinstance(value, str) else value
        if not isinstance(result, datetime) or result.tzinfo is None:
            raise ValueError
        return result.astimezone(UTC)
    raise ValueError("Unknown converter")


@dataclass(frozen=True)
class FieldRule:
    target: str
    paths: tuple[str, ...]
    conversion: str = "identity"
    required: bool = False
    default: object = MISSING

    def read(self, item):
        values = [path_value(item, path) for path in self.paths]
        found = [value for value in values if value is not MISSING]
        if len(found) > 1 and any(value != found[0] for value in found[1:]):
            raise MappingError("mapping.ambiguous_alias." + self.target)
        value = found[0] if found else self.default
        if value is MISSING or value is None:
            if self.required:
                raise MappingError("mapping.required." + self.target)
            return None
        try:
            return convert(value, self.conversion)
        except (ValueError, TypeError, InvalidOperation, OverflowError):
            raise MappingError("mapping.invalid_type." + self.target) from None


@dataclass(frozen=True)
class MappingSpec:
    version: str
    fields: tuple[FieldRule, ...]
    source_schema_versions: tuple[str, ...]

    def apply(self, item: dict) -> tuple[dict, list[str]]:
        values, reasons = {}, []
        for rule in self.fields:
            try:
                values[rule.target] = rule.read(item)
            except MappingError as error:
                reasons.append(error.code)
        return values, reasons

    def identity(self, item):
        rule = next(rule for rule in self.fields if rule.target == "identity")
        try:
            return rule.read(item)
        except MappingError:
            return None
