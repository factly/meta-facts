import asyncio
from typing import ChainMap, List
from urllib.parse import urlparse

from fastapi.logger import logger
from starlette.datastructures import UploadFile

from app.core.config import Settings
from app.models.enums import SourceType
from app.models.meta_data import MetaData
from app.utils.columns_mapping import find_mapped_columns
from app.utils.common import (
    get_dataset_from_file,
    get_dataset_from_s3,
    get_dataset_from_url,
)
from app.utils.formats_available import get_formats_available
from app.utils.granularity import get_granularity
from app.utils.is_public import get_is_public
from app.utils.output_file_name import (
    get_output_file_name,
    get_output_file_path,
)
from app.utils.s3_files import get_list_of_s3_objects, get_s3_resource
from app.utils.spatial_coverage import get_spatial_coverage
from app.utils.temporal_coverage import get_temporal_coverage
from app.utils.units import get_units

settings = Settings()


async def get_dataset_meta_data(dataset_full_path: str, session=None):
    try:
        dataset = await get_dataset_from_url(
            session=session, url=dataset_full_path
        )
    except Exception as e:
        logger.exception(
            f"Could not get datasets from: {dataset_full_path} : {e}"
        )
        logger.warning(f"Generate Blank MetaData for: {dataset_full_path}")
        return {
            dataset_full_path: MetaData(
                **await get_output_file_name(dataset_full_path)
            ).dict()
        }
    mapped_columns = await find_mapped_columns(dataset.columns)
    result = await asyncio.gather(
        get_output_file_path(
            dataset_full_path, source_type=SourceType.LOCAL.value
        ),
        get_units(dataset, mapped_columns),
        get_temporal_coverage(dataset, mapped_columns),
        get_granularity(dataset.columns),
        get_spatial_coverage(dataset),
        get_formats_available(dataset_full_path),
        get_is_public(dataset),
    )
    meta_data = dict(ChainMap(*result))
    return {dataset_full_path: meta_data}


async def create_meta_data_for_dataset_urls(urls: List[str], **kwargs) -> dict:
    tasks = [
        asyncio.ensure_future(get_dataset_meta_data(url, **kwargs))
        for url in urls
    ]

    results = await asyncio.gather(*tasks)
    return ChainMap(*results)


async def get_dataset_meta_data_for_file_object(file_object, filename: str):
    try:
        dataset = await get_dataset_from_file(file_object)
    except Exception as e:
        logger.exception(f"Could not get datasets from: {filename} : {e}")
        logger.warning(f"Generate Blank MetaData for: {filename}")
        return {
            filename: MetaData(**await get_output_file_name(filename)).dict()
        }
    else:
        mapped_columns = await find_mapped_columns(dataset.columns)
        result = await asyncio.gather(
            get_output_file_path(
                name=filename, source_type=SourceType.LOCAL.value
            ),
            get_units(dataset, mapped_columns),
            get_temporal_coverage(dataset, mapped_columns),
            get_granularity(dataset.columns),
            get_spatial_coverage(dataset),
            get_formats_available(filename),
            get_is_public(dataset),
        )
        meta_data = dict(ChainMap(*result))
        return {filename: meta_data}


async def create_meta_data_for_dataset_csv(
    csv_file_objects: List[UploadFile],
) -> dict:
    tasks = [
        asyncio.ensure_future(
            get_dataset_meta_data_for_file_object(
                csv_file.file, csv_file.filename
            )
        )
        for csv_file in csv_file_objects
    ]

    results = await asyncio.gather(*tasks)
    return ChainMap(*results)


async def get_dataset_meta_data_for_s3_file(s3_resource, s3_bucket, s3_key):
    try:
        dataset = await get_dataset_from_s3(
            s3_resource=s3_resource, s3_bucket=s3_bucket, s3_key=s3_key
        )
    except Exception as e:
        logger.exception(f"Could not get datasets from: {s3_key} : {e}")
        logger.warning(f"Generate Blank MetaData for: {s3_key}")
        return {s3_key: MetaData(**await get_output_file_name(s3_key)).dict()}
    else:
        logger.info(f"Data-frame created for {s3_key}")
        mapped_columns = await find_mapped_columns(dataset.columns)
        result = await asyncio.gather(
            get_output_file_path(
                name=s3_key,
                source_type=SourceType.S3.value,
                bucket_name=s3_bucket,
            ),
            get_units(dataset, mapped_columns),
            get_temporal_coverage(dataset, mapped_columns),
            get_granularity(dataset.columns),
            get_spatial_coverage(dataset),
            get_formats_available(s3_key),
            get_is_public(dataset),
        )
        meta_data = dict(ChainMap(*result))
        logger.info(f"Meta-data created for {s3_key}")
        return {s3_key: meta_data}


async def create_meta_data_for_s3_bucket(
    s3_resource, s3_bucket, prefix, file_format
):
    objects = await get_list_of_s3_objects(s3_resource, s3_bucket, prefix)
    tasks = [
        asyncio.ensure_future(
            get_dataset_meta_data_for_s3_file(
                s3_resource=s3_resource,
                s3_bucket=s3_bucket,
                s3_key=s3_file_obj.key,
            )
        )
        for s3_file_obj in objects
        if s3_file_obj.key.endswith(file_format)
    ]

    results = await asyncio.gather(*tasks)
    return ChainMap(*results)


async def create_meta_data_for_s3_files(s3_urls: List[str]):
    tasks = []
    s3_resource = await get_s3_resource(
        s3_access_key=settings.S3_SOURCE_ACCESS_KEY,
        s3_secret_key=settings.S3_SOURCE_SECRET_KEY,
        s3_endpoint_url=settings.S3_SOURCE_ENDPOINT_URL,
        resource="s3",
    )
    for s3_url in s3_urls:
        url_parts = urlparse(s3_url)
        s3_bucket, s3_key = url_parts.netloc, url_parts.path.lstrip("/")
        tasks.append(
            asyncio.ensure_future(
                get_dataset_meta_data_for_s3_file(
                    s3_resource=s3_resource,
                    s3_bucket=s3_bucket,
                    s3_key=s3_key,
                )
            )
        )

    results = await asyncio.gather(*tasks)
    return ChainMap(*results)
