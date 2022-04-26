import asyncio
import io
import logging
from typing import ChainMap, List

import pandas as pd
from fastapi.logger import logger
from starlette.datastructures import UploadFile

from app.models.meta_data import MetaData
from app.utils.columns_mapping import find_mapped_columns
from app.utils.common import get_dataset
from app.utils.formats_available import get_formats_available
from app.utils.granularity import get_granularity
from app.utils.is_public import get_is_public
from app.utils.output_file_name import get_output_file_name
from app.utils.spatial_coverage import get_spatial_coverage
from app.utils.temporal_coverage import get_temporal_coverage
from app.utils.units import get_units

logging.basicConfig(level=logging.DEBUG)


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


async def create_meta_data_for_s3_csv(s3_client, bucket_name, object_name):
    try:
        response = s3_client.get_object(bucket_name, object_name)
    # Read data from response.
    except Exception as e:
        logger.exception(f"Could not get object from S3 : {e}")
    else:
        print(type(response))
        print(response)
    finally:
        response.close()
        response.release_conn()


async def create_meta_data_for_pandas_dataframe(
    dataframe: pd.DataFrame, dataset_full_path: str
):
    mapped_columns = await find_mapped_columns(dataframe.columns)
    result = await asyncio.gather(
        get_output_file_name(dataset_full_path),
        get_units(dataframe, mapped_columns),
        get_temporal_coverage(dataframe, mapped_columns),
        get_granularity(dataframe.columns),
        get_spatial_coverage(dataframe),
        get_formats_available(dataset_full_path),
        get_is_public(dataframe),
    )
    meta_data = dict(ChainMap(*result))
    return {dataset_full_path: meta_data}


async def create_meta_data_for_s3_objects(s3, bucket_name: str, object):
    try:
        logger.debug(f"File object to download: {object._object_name}")
        response = s3.get_object(bucket_name, object._object_name)
    except Exception as e:
        logger.exception(f"error while downloading file : {e}")
        meta = {object._object_name: {}}
    else:
        # read s3 files for csv as dataframes
        data = pd.read_csv(io.BytesIO(response.data), encoding="utf8")
        meta = await create_meta_data_for_pandas_dataframe(
            data, object._object_name
        )
    finally:
        response.close()
        response.release_conn()
        return meta
