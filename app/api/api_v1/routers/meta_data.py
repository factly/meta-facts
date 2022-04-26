import asyncio
from typing import ChainMap, Dict, List

from botocore.client import BaseClient
from fastapi import Depends, FastAPI, File, Form, HTTPException, UploadFile
from minio.error import S3Error
from starlette import status

from app.core.config import Settings
from app.models.meta_data import MetaData
from app.utils.dependency import s3_auth
from app.utils.meta_data import (
    create_meta_data_for_dataset_csv,
    create_meta_data_for_dataset_urls,
    create_meta_data_for_s3_objects,
)

settings = Settings()

meta_data_router = router = FastAPI(
    title="Routes to get Meta for dataset files",
    description="Mentioned api helps to find meta data for specific or all attributes in a dataset.",
)


@router.post("/urls", response_model=Dict[str, MetaData])
async def get_metadata_for_dataset_with_source_urls(urls: List[str]):
    """Functions Facilitates to generate meta-data for datasets when their download link are provided."""
    meta_data = await create_meta_data_for_dataset_urls(urls)
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


@router.post("/s3", response_model=Dict[str, MetaData])
async def create_meta_data_for_dataset_from_s3(
    bucket_name: str = Form(
        "election-commision-india", description="Name of the bucket"
    ),
    prefix: str = Form("processed", description="Name of the prefix"),
    s3: BaseClient = Depends(s3_auth),
):
    """Functions Facilitates to generate meta-data for datasets when file objects link is provided."""
    try:
        objects = s3.list_objects(
            bucket_name=bucket_name, prefix=prefix, recursive=True
        )
    except S3Error as s3e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail=f"{s3e}"
        )
    else:
        result = await asyncio.gather(
            *[
                create_meta_data_for_s3_objects(s3, bucket_name, object)
                for object in objects
            ]
        )
        meta_data = dict(ChainMap(*result))
        return meta_data
