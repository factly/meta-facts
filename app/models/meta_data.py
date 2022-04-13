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


"""
[
"https://storage.factly.org/mande/edu-ministry/data/processed/statistics/1_AISHE_report/1_universities_count_by_state/output.csv",
"https://storage.factly.org/mande/edu-ministry/data/processed/statistics/1_AISHE_report/19_enrolment_foreign/output.csv"
]
"""
