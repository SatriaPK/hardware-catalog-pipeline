from sqlalchemy import URL, create_engine

from marketplace_pipeline.settings import Settings


def postgres_url(settings: Settings) -> URL:
    return URL.create(
        "postgresql+psycopg",
        username=settings.db_user,
        password=settings.db_password,
        host=settings.db_host,
        port=settings.db_port,
        database=settings.db_name,
    )


def postgres_engine(settings: Settings):
    return create_engine(postgres_url(settings), connect_args={"connect_timeout": 5})


def minio_client(settings: Settings):
    import boto3
    from botocore.config import Config

    if not settings.minio_access_key or not settings.minio_secret_key:
        raise ValueError("Missing explicit storage credentials")
    # Explicit credentials prevent credential discovery/instance-metadata requests.
    return boto3.client(
        "s3",
        endpoint_url=settings.minio_endpoint,
        aws_access_key_id=settings.minio_access_key,
        aws_secret_access_key=settings.minio_secret_key,
        region_name="us-east-1",
        config=Config(
            connect_timeout=5,
            read_timeout=5,
            retries={"max_attempts": 1},
            s3={"addressing_style": "path"},
        ),
    )
