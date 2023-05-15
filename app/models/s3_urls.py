from typing import List

from pydantic import BaseModel, Field


class S3Urls(BaseModel):
    urls: List[str] = Field(...)

    class Config:
        schema_extra = {
            "example": {
                "urls": [
                    "s3://roapitest/processed/rice/2015/output.csv",
                    "s3://roapitest/processed/rice/2016/output.csv",
                ]
            }
        }
