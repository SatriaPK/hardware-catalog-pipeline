import hashlib
import json
import math
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from pydantic import ValidationError

from marketplace_pipeline.domain import CanonicalListing
from marketplace_pipeline.mapping import MISSING, normalize, path_value
from marketplace_pipeline.providers import registry

# Conservative subset of ISO 4217. Unsupported currencies are quarantined, not guessed.
CURRENCIES = frozenset("USD JPY HKD SGD IDR EUR GBP CNY KRW AUD CAD CHF TWD".split())


def hash_fields(fields: dict) -> str:
    return hashlib.sha256(
        json.dumps(
            fields, sort_keys=True, ensure_ascii=False, separators=(",", ":"), default=str
        ).encode()
    ).hexdigest()


def map_item(item: dict, context: dict) -> tuple[CanonicalListing | None, list[str], int]:
    source = context["source"]
    provider = registry.get(source)
    version = context.get("mapping_version", provider.current_mapping)
    spec = provider.mapping(version)
    values, reasons = spec.apply(item)
    identity, title = values.get("identity"), values.get("title")
    amount, currency = values.get("amount"), values.get("currency")
    policy = provider.policy
    if policy.default_currency:
        if context["marketplace_id"] != policy.currency_marketplace:
            currency = None
        elif all(
            path_value(item, path) is MISSING
            for rule in spec.fields
            if rule.target == "currency"
            for path in rule.paths
        ):
            currency = policy.default_currency
    observed = context["observed_at"]
    required = [source, context["marketplace_id"], identity, title, amount, currency, observed]
    present = sum(v is not None and v != "" for v in required)
    if not isinstance(identity, str) or not identity.strip():
        reasons.append("missing_or_invalid_listing_id")
    if not isinstance(title, str) or not normalize(title):
        reasons.append("missing_or_invalid_title")
    if not isinstance(currency, str) or currency not in CURRENCIES:
        reasons.append("unsupported_or_invalid_currency")
    if policy.required_currency and currency != policy.required_currency:
        reasons.append("currency_marketplace_mismatch")
    if context["category"] not in policy.categories:
        reasons.append("unknown_category")
    parsed: Decimal | None = None
    try:
        if isinstance(amount, bool) or not isinstance(amount, (str, int, Decimal)):
            raise ValueError
        parsed = Decimal(amount)
        if not parsed.is_finite() or parsed < 0 or parsed >= Decimal("100000000000000"):
            raise ValueError
        if parsed == 0:
            reasons.append("suspicious_zero_price")
    except (InvalidOperation, ValueError):
        reasons.append("invalid_price")
    if not isinstance(observed, datetime) or observed.tzinfo is None:
        reasons.append("invalid_observed_at")
    ingested = context["ingested_at"]
    if not isinstance(ingested, datetime) or ingested.tzinfo is None:
        reasons.append("invalid_ingested_at")
    if reasons:
        return None, reasons, present
    assert isinstance(title, str) and isinstance(identity, str) and parsed is not None
    assert isinstance(observed, datetime) and isinstance(ingested, datetime)
    business: dict[str, Any] = {
        "title_normalized": normalize(title),
        "category_canonical": context["category"],
        "price_amount": parsed,
        "currency_code": currency,
        **{
            key: normalize(values[key])
            if isinstance(values.get(key), str) and values[key].strip()
            else None
            for key in ("brand", "model", "variant")
        },
    }
    # Decimal values with equivalent scale must produce the same business hash.
    hashed = {**business, "price_amount": format(parsed, ".4f")}
    try:
        listing = CanonicalListing(
            source=source,
            marketplace_id=context["marketplace_id"],
            listing_id=identity,
            run_id=context["run_id"],
            raw_request_id=context["request_id"],
            observed_at_utc=observed.astimezone(UTC),
            ingested_at_utc=ingested.astimezone(UTC),
            title_original=title,
            record_hash=hash_fields(hashed),
            schema_version=context.get("canonical_schema_version", 2),
            mapping_version=version,
            attributes_json={} if version == "legacy-1" else json_safe(item),
            **business,
        )
    except (ValidationError, ValueError):
        return None, ["canonical_schema_violation"], present
    return listing, [], present


def json_safe(value):
    """Preserve source fields; exact Decimal values are represented as strings."""
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError("Non-finite extension")
        return str(value)
    if isinstance(value, dict):
        if any(not isinstance(key, str) for key in value):
            raise ValueError("Non-string extension key")
        return {key: json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [json_safe(item) for item in value]
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError("Non-finite extension")
    return value
