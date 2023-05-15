import boto3
from fastapi import HTTPException, status

from app.core.config import Settings

settings = Settings()


async def get_s3_resource(
    s3_access_key: str, s3_secret_key: str, s3_endpoint_url: str, resource: str
):
    s3_access_key = (
        settings.S3_SOURCE_ACCESS_KEY
        if s3_access_key is None
        else s3_access_key
    )
    s3_secret_key = (
        settings.S3_SOURCE_SECRET_KEY
        if s3_secret_key is None
        else s3_secret_key
    )
    s3_endpoint_url = (
        settings.S3_SOURCE_ENDPOINT_URL
        if s3_endpoint_url is None
        else s3_endpoint_url
    )
    resource = settings.S3_SOURCE_RESOURCE if resource is None else resource
    try:
        session = boto3.Session(
            aws_access_key_id=s3_access_key,
            aws_secret_access_key=s3_secret_key,
        )

        s3_resource = session.resource(resource, endpoint_url=s3_endpoint_url)
    except Exception as e:
        raise ValueError(f"Error connecting to S3: {e}")
    else:
        return s3_resource


async def get_list_of_s3_objects(s3_resource, s3_bucket, prefix):
    try:
        if prefix:
            s3_objects = s3_resource.Bucket(s3_bucket).objects.filter(
                Prefix=prefix
            )
        else:
            s3_objects = s3_resource.Bucket(s3_bucket).objects.all()
    except Exception as e:
        raise ValueError(f"Error getting list of S3 objects: {e}")
    else:
        return s3_objects


async def get_s3_object(s3_resource, s3_bucket, s3_key):
    try:
        s3_object = s3_resource.Object(s3_bucket, s3_key)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail=f"{e}"
        )
    else:
        return s3_object
