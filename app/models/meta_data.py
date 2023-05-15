from typing import Optional

from pydantic import BaseModel


class MetaData(BaseModel):
    output_file_name: Optional[str]
    units: Optional[str]
    temporal_coverage: Optional[str]
    granularity: Optional[str]
    spatial_coverage: Optional[str]
    formats_available: Optional[str]
    is_public: Optional[bool]

    class Config:
        order = True
