import json
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from marketplace_pipeline.domain import CanonicalListing

SCHEMA = pa.schema(
    [
        (name, pa.string())
        for name in (
            "source",
            "marketplace_id",
            "listing_id",
            "run_id",
            "raw_request_id",
            "title_original",
            "title_normalized",
            "category_canonical",
            "currency_code",
            "record_hash",
            "category_source",
            "brand",
            "model",
            "variant",
            "gtin",
            "item_url",
            "attributes_json",
        )
    ]
    + [
        ("price_amount", pa.decimal128(18, 4)),
        ("observed_at_utc", pa.timestamp("us", tz="UTC")),
        ("ingested_at_utc", pa.timestamp("us", tz="UTC")),
        ("schema_version", pa.int32()),
    ],
    metadata={b"schema_version": b"1"},
)


def encode(rows: list[CanonicalListing]) -> bytes:
    records: list[dict[str, Any]] = []
    for row in rows:
        values = row.model_dump()
        values["attributes_json"] = json.dumps(values["attributes_json"], sort_keys=True)
        records.append(values)
    sink = pa.BufferOutputStream()
    pq.write_table(
        pa.Table.from_pylist(records, schema=SCHEMA), sink, compression="zstd", version="2.6"
    )
    return sink.getvalue().to_pybytes()


def decode(body: bytes) -> list[CanonicalListing]:
    table = pq.read_table(pa.BufferReader(body))
    if not table.schema.equals(SCHEMA, check_metadata=True):
        raise ValueError("Unexpected CLEAN schema")
    rows = []
    for record in table.to_pylist():
        record["attributes_json"] = json.loads(record["attributes_json"])
        rows.append(CanonicalListing.model_validate(record))
    return rows
