import asyncio
from typing import ChainMap, List

from fastapi.logger import logger
from starlette.datastructures import UploadFile

from app.models.meta_data import MetaData
from app.utils.columns_mapping import find_mapped_columns
from app.utils.common import get_dataset, get_dataset_from_s3
from app.utils.formats_available import get_formats_available
from app.utils.granularity import get_granularity
from app.utils.is_public import get_is_public
from app.utils.output_file_name import get_output_file_name
from app.utils.s3_files import get_list_of_s3_objects
from app.utils.spatial_coverage import get_spatial_coverage
from app.utils.temporal_coverage import get_temporal_coverage
from app.utils.units import get_units


async def get_dataset_meta_data(dataset_full_path: str):
    try:
        dataset = await get_dataset(dataset_full_path)
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
        get_output_file_name(dataset_full_path),
        get_units(dataset, mapped_columns),
        get_temporal_coverage(dataset, mapped_columns),
        get_granularity(dataset.columns),
        get_spatial_coverage(dataset),
        get_formats_available(dataset_full_path),
        get_is_public(dataset),
    )
    meta_data = dict(ChainMap(*result))
    return {dataset_full_path: meta_data}


async def create_meta_data_for_dataset_urls(urls: List[str]) -> dict:
    tasks = [asyncio.ensure_future(get_dataset_meta_data(url)) for url in urls]

    results = await asyncio.gather(*tasks)
    return ChainMap(*results)


async def get_dataset_meta_data_for_file_object(file_object, filename: str):
    try:
        dataset = await get_dataset(file_object)
    except Exception as e:
        logger.exception(f"Could not get datasets from: {filename} : {e}")
        logger.warning(f"Generate Blank MetaData for: {filename}")
        return {
            filename: MetaData(**await get_output_file_name(filename)).dict()
        }
    else:
        mapped_columns = await find_mapped_columns(dataset.columns)
        result = await asyncio.gather(
            get_output_file_name(filename),
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
            get_output_file_name(s3_key),
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


async def create_meta_data_for_s3_bucket(s3_resource, s3_bucket, prefix):

    tasks = [
        asyncio.ensure_future(
            get_dataset_meta_data_for_s3_file(
                s3_resource=s3_resource,
                s3_bucket=s3_bucket,
                s3_key=s3_file_obj.key,
            )
        )
        for s3_file_obj in get_list_of_s3_objects(
            s3_resource, s3_bucket, prefix
        )
    ]

    results = await asyncio.gather(*tasks)
    return ChainMap(*results)
