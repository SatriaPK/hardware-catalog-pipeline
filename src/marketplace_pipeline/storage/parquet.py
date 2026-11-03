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


SCHEMA_V2 = pa.schema(
    list(SCHEMA) + [pa.field("mapping_version", pa.string()), pa.field("entity_kind", pa.string())],
    metadata={b"schema_version": b"2"},
)


def encode(rows: list[CanonicalListing], *, version: int = 2) -> bytes:
    schema = {1: SCHEMA, 2: SCHEMA_V2}[version]
    records: list[dict[str, Any]] = []
    for row in rows:
        if row.schema_version != version or (version == 1 and row.mapping_version != "legacy-1"):
            raise ValueError("CLEAN row version differs from output schema")
        values = row.model_dump()
        values["attributes_json"] = json.dumps(values["attributes_json"], sort_keys=True)
        records.append(values)
    sink = pa.BufferOutputStream()
    pq.write_table(
        pa.Table.from_pylist(records, schema=schema), sink, compression="zstd", version="2.6"
    )
    return sink.getvalue().to_pybytes()


def decode(body: bytes) -> list[CanonicalListing]:
    table = pq.read_table(pa.BufferReader(body))
    if not any(table.schema.equals(schema, check_metadata=True) for schema in (SCHEMA, SCHEMA_V2)):
        raise ValueError("Unexpected CLEAN schema")
    rows = []
    for record in table.to_pylist():
        if str(record["schema_version"]).encode() != table.schema.metadata[b"schema_version"]:
            raise ValueError("CLEAN row version differs from file schema")
        record["attributes_json"] = json.loads(record["attributes_json"])
        rows.append(CanonicalListing.model_validate(record))
    return rows
