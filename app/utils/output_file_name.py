from pathlib import Path
from typing import Dict, Union

from app.models.enums import SourceType


async def get_output_file_name(full_file_path: str):
    home_user = str(Path(full_file_path).home())
    output_file_name = full_file_path.split("/projects/")[-1].split(home_user)[
        -1
    ]
    return {"output_file_name": output_file_name}


async def get_output_file_path(
    name: str, source_type: SourceType, bucket_name: Union[str, None] = None
) -> Dict[str, str]:
    """Return file path for given source type. For local files return name ,
        but for s3 object return full path with proper s3 convention url.

    Args:
        name (str): file name or s3-key
        source_type (SourceType): file taken from local or s3
        bucket_name (str): for s3 object provide the s3 bucket name

    Returns:
        Dict[str, str]: return full path
    """
    if source_type == SourceType.S3.value:
        output_file_path = f"s3://{bucket_name}/{name}"
    else:
        output_file_path = f"{name}"

    return {"output_file_name": output_file_path}
