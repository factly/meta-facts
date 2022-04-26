from botocore.client import BaseClient
from minio import Minio

from app.core.config import Settings

settings = Settings()


def s3_auth() -> BaseClient:
    s3 = Minio(
        endpoint=settings.S3_SERVER_ENDPOINT,
        access_key=settings.S3_SERVER_PUBLIC_KEY,
        secret_key=settings.S3_SERVER_SECRET_KEY,
        secure=settings.S3_SECURE_CONNECTION,
    )
    return s3
