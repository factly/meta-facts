import boto3
from botocore.client import BaseClient

from app.core.config import Settings

settings = Settings()


def s3_auth() -> BaseClient:
    s3 = boto3.client(
        service_name="s3",
        aws_access_key_id=settings.AWS_SERVER_PUBLIC_KEY,
        aws_secret_access_key=settings.AWS_SERVER_SECRET_KEY,
        endpoint_url=settings.AWS_SERVER_ENDPOINT,
    )
    return s3
