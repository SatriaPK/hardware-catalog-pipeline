import hashlib
import json
import unicodedata
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from pydantic import ValidationError

from marketplace_pipeline.domain import CanonicalListing

# Conservative subset of ISO 4217. Unsupported currencies are quarantined, not guessed.
CURRENCIES = frozenset("USD JPY HKD SGD IDR EUR GBP CNY KRW AUD CAD CHF TWD".split())
CATEGORIES = frozenset("gpu ram ssd keyboard mouse monitor".split())


def normalize(value: str) -> str:
    return " ".join(unicodedata.normalize("NFC", value).split())


def hash_fields(fields: dict) -> str:
    return hashlib.sha256(
        json.dumps(
            fields, sort_keys=True, ensure_ascii=False, separators=(",", ":"), default=str
        ).encode()
    ).hexdigest()


def map_item(item: dict, context: dict) -> tuple[CanonicalListing | None, list[str], int]:
    source = context["source"]
    ebay = source == "ebay"
    identity = item.get("itemId" if ebay else "itemCode")
    title = item.get("title" if ebay else "itemName")
    price = item.get("price") if ebay else None
    amount = price.get("value") if isinstance(price, dict) else None
    currency = price.get("currency") if isinstance(price, dict) else None
    if not ebay:
        amount = item.get("itemPrice")
        # Explicit Japanese Ichiba context permits JPY; other marketplaces never infer it.
        currency = (
            item.get("currency", "JPY") if context["marketplace_id"] == "RAKUTEN_JP" else None
        )
    observed = context["observed_at"]
    required = [source, context["marketplace_id"], identity, title, amount, currency, observed]
    present = sum(v is not None and v != "" for v in required)
    reasons = []
    if not isinstance(identity, str) or not identity.strip():
        reasons.append("missing_or_invalid_listing_id")
    if not isinstance(title, str) or not normalize(title):
        reasons.append("missing_or_invalid_title")
    if not isinstance(currency, str) or currency not in CURRENCIES:
        reasons.append("unsupported_or_invalid_currency")
    if not ebay and currency != "JPY":
        reasons.append("currency_marketplace_mismatch")
    if context["category"] not in CATEGORIES:
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
            key: normalize(item[key])
            if isinstance(item.get(key), str) and item[key].strip()
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
            **business,
        )
    except ValidationError:
        return None, ["canonical_schema_violation"], present
    return listing, [], present
