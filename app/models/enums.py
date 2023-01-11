from enum import Enum


class SourceType(str, Enum):
    S3 = "S3"
    LOCAL = "LOCAL"
