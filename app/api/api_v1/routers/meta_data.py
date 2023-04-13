from typing import Dict, List, Union

from aiohttp import ClientSession
from fastapi import FastAPI, File, Form, HTTPException, UploadFile, status
from fastapi.logger import logger

from app.core.config import Settings
from app.models.meta_data import MetaData
from app.models.s3_urls import S3Urls
from app.utils.meta_data import (
    create_meta_data_for_dataset_csv,
    create_meta_data_for_dataset_urls,
    create_meta_data_for_s3_bucket,
    create_meta_data_for_s3_file,
)
from app.utils.s3_files import get_list_of_s3_objects, get_s3_resource

settings = Settings()

meta_data_router = router = FastAPI(
    title="Routes to get Meta for dataset files",
    description="Mentioned api helps to find meta data for specific or all attributes in a dataset.",
)


@router.post("/urls", response_model=Dict[str, MetaData])
async def get_metadata_for_dataset_with_source_urls(urls: List[str]):
    """Functions Facilitates to generate meta-data for datasets when their download link are provided."""
    session = ClientSession()
    meta_data = await create_meta_data_for_dataset_urls(urls, session=session)
    await session.close()
    return meta_data


@router.post("/files", response_model=Dict[str, MetaData])
async def get_meta_data_from_files(
    csv_files: List[UploadFile] = File(
        ..., description="List of dataset files in csv format"
    ),
):
    """Functions Facilitates to generate meta-data for datasets when file objects link is provided."""
    meta_data = await create_meta_data_for_dataset_csv(csv_files)
    return meta_data


@router.post("/s3")
async def get_meta_data_from_s3(
    s3_bucket: str = Form(..., description="S3 bucket name"),
    prefix: Union[str, None] = Form(
        default=None,
        description="S3 file prefix, to list down all the files under particular prefix",
    ),
    s3_access_key: Union[str, None] = Form(None, description="S3 access key"),
    s3_secret_key: Union[str, None] = Form(None, description="S3 secret key"),
    s3_endpoint_url: Union[str, None] = Form(
        None, description="S3 endpoint url"
    ),
    resource: Union[str, None] = Form(None, description="S3 resource"),
    file_format: str = Form("csv", description="Format of the processed file"),
):
    """Functions Facilitates to generate meta-data for datasets when file objects link is provided."""
    try:
        s3_resource = await get_s3_resource(
            s3_access_key=s3_access_key,
            s3_secret_key=s3_secret_key,
            s3_endpoint_url=s3_endpoint_url,
            resource=resource,
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Error connecting to S3: {e}",
        )
    else:
        logger.info(f"Connected to S3 bucket: {s3_bucket}")
        meta_data = await create_meta_data_for_s3_bucket(
            s3_resource=s3_resource,
            s3_bucket=s3_bucket,
            prefix=prefix,
            file_format=file_format,
        )
        return meta_data


@router.post(
    "/s3/urls",
    response_model=Dict[str, MetaData],
    description="Get meta data for S3 files from their urls",
)
async def get_meta_data_from_s3_urls(
    source: S3Urls,
):
    try:
        urls = source.urls
        logger.info(f"Getting meta data for S3 files: {len(urls)}")
        meta_data = await create_meta_data_for_s3_file(
            s3_urls=urls,
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Error getting meta data for S3 files: {e}",
        )
    else:
        return meta_data


@router.post("/s3/files/list")
async def list_bucket_objects(
    s3_bucket: str = Form(..., description="S3 bucket name"),
    prefix: Union[str, None] = Form(
        default=None,
        description="S3 file prefix, to list down all the files under particular prefix",
    ),
    s3_access_key: Union[str, None] = Form(None, description="S3 access key"),
    s3_secret_key: Union[str, None] = Form(None, description="S3 secret key"),
    s3_endpoint_url: Union[str, None] = Form(
        None, description="S3 endpoint url"
    ),
    resource: Union[str, None] = Form(None, description="S3 resource"),
    file_format: str = Form("csv", description="Format of the processed file"),
):
    try:
        s3_resource = await get_s3_resource(
            s3_access_key=s3_access_key,
            s3_secret_key=s3_secret_key,
            s3_endpoint_url=s3_endpoint_url,
            resource=resource,
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Error connecting to S3: {e}",
        )
    else:
        objects = await get_list_of_s3_objects(s3_resource, s3_bucket, prefix)
        objects_json = [
            {
                "key": obj.key,
                "last_modified": obj.last_modified,
                "size": obj.size / 1e3,
            }
            for obj in objects
            if obj.key.endswith(file_format)
        ]
        return {
            "total": len(objects_json),
            "file_size": "KB",
            "bucket": s3_bucket,
            "objects": objects_json,
        }
