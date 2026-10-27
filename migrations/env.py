"""Offline SQL by default; init supplies the namespace-scoped online connection."""

from alembic import context

if context.is_offline_mode():
    context.configure(url="postgresql+psycopg://", literal_binds=True)
    with context.begin_transaction():
        context.run_migrations()
else:
    connection = context.config.attributes.get("connection")
    if connection is None:
        raise RuntimeError("Use marketpipe init; no automatic host DB access")
    context.configure(
        connection=connection,
        version_table_schema=context.config.attributes.get("namespace"),
    )
    with context.begin_transaction():
        context.run_migrations()
