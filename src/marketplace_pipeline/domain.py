from decimal import Decimal
from typing import Any, Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field


class CanonicalListing(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    source: Literal["ebay", "rakuten"]
    marketplace_id: str = Field(min_length=1)
    listing_id: str = Field(min_length=1)
    run_id: str = Field(min_length=1)
    raw_request_id: str = Field(min_length=1)
    observed_at_utc: AwareDatetime
    ingested_at_utc: AwareDatetime
    title_original: str = Field(min_length=1)
    title_normalized: str = Field(min_length=1)
    category_canonical: Literal["gpu", "ram", "ssd", "keyboard", "mouse", "monitor", "unknown"]
    price_amount: Decimal = Field(ge=0, max_digits=18, decimal_places=4, strict=True)
    currency_code: str = Field(pattern=r"^[A-Z]{3}$")
    record_hash: str = Field(pattern=r"^[a-f0-9]{64}$")
    category_source: str | None = None
    brand: str | None = None
    model: str | None = None
    variant: str | None = None
    gtin: str | None = None
    item_url: str | None = None
    attributes_json: dict[str, Any] = Field(default_factory=dict)
    schema_version: Literal[1] = 1
