from typing import Dict, List

from fastapi import FastAPI, File, UploadFile

from app.core.config import Settings
from app.models.meta_data import MetaData
from app.utils.meta_data import (
    create_meta_data_for_dataset_csv,
    create_meta_data_for_dataset_urls,
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
