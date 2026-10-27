"""PostgreSQL transaction boundary used by the acquisition pipeline."""

import re
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from sqlalchemy import MetaData, Table, and_, func, select, text, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import Connection, Engine

from marketplace_pipeline.storage.object_store import IntegrityConflict

TABLES = (
    "pipeline_run",
    "run_plan",
    "ingest_request",
    "dim_marketplace",
    "dim_listing",
    "fact_price_snapshot",
    "acquisition_output",
    "pipeline_checkpoint",
    "acquisition_invocation",
    "acquisition_page",
    "acquisition_occurrence",
)


class PostgresTransaction:
    def __init__(self, connection: Connection, tables: dict[str, Table]):
        self.connection, self.tables = connection, tables

    def rows(self, table: str, **where: Any) -> list[dict[str, Any]]:
        target = self.tables[table]
        query = select(target).where(and_(True, *(target.c[k] == v for k, v in where.items())))
        return [dict(row) for row in self.connection.execute(query).mappings()]

    def count(self, table: str) -> int:
        return self.connection.scalar(select(func.count()).select_from(self.tables[table])) or 0

    def put(self, table: str, key: dict, values: dict) -> bool:
        target = self.tables[table]
        values = {**key, **values}
        result = self.connection.execute(
            insert(target).values(**values).on_conflict_do_nothing().returning(*target.primary_key)
        ).first()
        actual = self.rows(table, **key)
        if len(actual) != 1 or any(actual[0][k] != v for k, v in values.items()):
            raise IntegrityConflict(f"Immutable {table} row conflict")
        return result is not None

    def change(self, table: str, key: dict, values: dict) -> None:
        target = self.tables[table]
        self.connection.execute(
            update(target).where(and_(*(target.c[k] == v for k, v in key.items()))).values(**values)
        )


class PostgresRepository:
    def __init__(self, engine: Engine, namespace: str):
        self.engine = engine
        if not re.fullmatch(r"mp_acq_[a-z0-9]{3,24}", namespace):
            raise ValueError("Invalid pipeline namespace")
        self.namespace = namespace
        metadata = MetaData(schema=namespace)
        self.tables = {name: Table(name, metadata, autoload_with=engine) for name in TABLES}

    @contextmanager
    def transaction(self) -> Iterator[PostgresTransaction]:
        with self.engine.begin() as connection:
            # Namespace-scoped serialization covers concurrent replay and shared listing updates.
            connection.execute(
                text("SELECT pg_advisory_xact_lock(hashtextextended(:name, 0))"),
                {"name": self.namespace},
            )
            yield PostgresTransaction(connection, self.tables)
