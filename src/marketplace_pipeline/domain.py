from decimal import Decimal
from typing import Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, JsonValue


class CanonicalListing(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    source: str = Field(pattern=r"^[a-z][a-z0-9_-]{0,63}$")
    marketplace_id: str = Field(min_length=1)
    listing_id: str = Field(min_length=1)
    run_id: str = Field(min_length=1)
    raw_request_id: str = Field(min_length=1)
    observed_at_utc: AwareDatetime
    ingested_at_utc: AwareDatetime
    title_original: str = Field(min_length=1)
    title_normalized: str = Field(min_length=1)
    category_canonical: str = Field(min_length=1)
    price_amount: Decimal = Field(ge=0, max_digits=18, decimal_places=4, strict=True)
    currency_code: str = Field(pattern=r"^[A-Z]{3}$")
    record_hash: str = Field(pattern=r"^[a-f0-9]{64}$")
    category_source: str | None = None
    brand: str | None = None
    model: str | None = None
    variant: str | None = None
    gtin: str | None = None
    item_url: str | None = None
    attributes_json: dict[str, JsonValue] = Field(default_factory=dict)
    schema_version: Literal[1, 2] = 1
    mapping_version: str = "legacy-1"
    entity_kind: Literal["listing"] = "listing"

    def envelope(self) -> "CanonicalEnvelope":
        return CanonicalEnvelope(
            source_id=self.source,
            marketplace_id=self.marketplace_id,
            source_record_id=self.listing_id,
            run_id=self.run_id,
            raw_request_id=self.raw_request_id,
            observed_at_utc=self.observed_at_utc,
            ingested_at_utc=self.ingested_at_utc,
            schema_version=self.schema_version,
            mapping_version=self.mapping_version,
            record_hash=self.record_hash,
            normalized_payload=ListingPayload(
                **self.model_dump(include=set(ListingPayload.model_fields))
            ),
            extensions=self.attributes_json,
        )


class ListingPayload(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    title_original: str = Field(min_length=1)
    title_normalized: str = Field(min_length=1)
    category_canonical: str = Field(min_length=1)
    price_amount: Decimal = Field(ge=0, max_digits=18, decimal_places=4, strict=True)
    currency_code: str = Field(pattern=r"^[A-Z]{3}$")
    category_source: str | None = None
    brand: str | None = None
    model: str | None = None
    variant: str | None = None
    gtin: str | None = None
    item_url: str | None = None


class CanonicalEnvelope(BaseModel):
    """Listing is the only currently supported entity; new kinds need typed models."""

    model_config = ConfigDict(frozen=True, extra="forbid")
    source_id: str = Field(pattern=r"^[a-z][a-z0-9_-]{0,63}$")
    marketplace_id: str = Field(min_length=1)
    entity_kind: Literal["listing"] = "listing"
    source_record_id: str = Field(min_length=1)
    run_id: str = Field(min_length=1)
    raw_request_id: str = Field(min_length=1)
    observed_at_utc: AwareDatetime
    ingested_at_utc: AwareDatetime
    schema_version: Literal[1, 2]
    mapping_version: str = Field(min_length=1)
    record_hash: str = Field(pattern=r"^[a-f0-9]{64}$")
    normalized_payload: ListingPayload
    extensions: dict[str, JsonValue] = Field(default_factory=dict)
