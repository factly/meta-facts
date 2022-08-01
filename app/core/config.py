from typing import List

from pydantic import BaseSettings


class Settings(BaseSettings):

    PROJECT_NAME: str = "Meta Facts"
    API_V1_STR: str = "/api/v1"
    MODE: str = "development"
    DOCS_URL: str = "/api/docs"
    # CORS PARAMS
    CORS_ORIGINS: list = [
        "http://127.0.0.1:8000",
        "http://127.0.0.1:8005",
        "http://127.0.0.1:4455",
        "http://localhost:8000",
        "http://localhost:8005",
        "http://localhost:4455/",
    ]
    CORS_METHODS: list = ["GET", "POST"]
    CORS_ALLOWED_CREDENTIALS: bool = True
    CORS_HEADERS: List[str] = ["*"]

    # Dataset source configurations
    S3_SOURCE_ACCESS_KEY: str = ...
    S3_SOURCE_SECRET_KEY: str = ...
    S3_SOURCE_ENDPOINT_URL: str = ...
    S3_SOURCE_RESOURCE: str = "S3"

    class Config:
        env_file = ".env"


class DateTimeSettings(BaseSettings):

    CALENDAR_YEAR_KEYWORD = "year"
    FISCAL_YEAR_KEYWORD = "fiscal_year"
    ACADEMIC_YEAR_KEYWORD = "academic_year"
    QUARTER_KEYWORD = "quarter"
    MONTH_KEYWORD = "month"
    DATE_KEYWORD = "date"
    CALENDAR_YEAR_PATTERN = "%Y"
    NON_CALENDAR_YEAR_PATTERN = r"^\d{4}-\d{2}$"
    QUARTER_FORMAT = r"Q[1-4]"
    MONTH_FORMAT = "%B"
    DATE_FORMAT = "%d-%m-%Y"
    ALL_YEAR_FORMATS = r"^\d{4}$|^\d{4}-\d{2,4}$"
    GRANULARITY_ORDER = {
        1: ["date"],
        2: ["week"],
        3: ["month"],
        4: ["quarter"],
        5: ["calender_year", "non_calendar_year"],
    }
    GRANULARITY_REPRESENTATION = {
        "date": "Daily",
        "week": "Weekly",
        "month": "Monthly",
        "quarter": "Quarterly",
        "calender_year": "Yearly",
        "non_calendar_year": "Yearly",
    }


class GeographySettings(BaseSettings):

    COUNTRY_KEYWORD = "country"
    STATE_KEYWORD = "state"
    DISTRICT_KEYWORD = "district"
    GRANULARITY_ORDER = {
        1: ["state"],
        2: ["country"],
        3: ["district"],
    }
    GRANULARITY_REPRESENTATION = {
        "district": "District",
        "state": "State",
        "country": "Country",
    }
    SPATIAL_COVERAGE_ORDER = ["country", "state", "district"]
    DEFAULT_SPATIAL_COVERAGE = "India"
    SPATIAL_COVERAGE_MULTIPLE_CONVENTION = {
        "country": "Country",
        "state": "States",
        "district": "Districts",
    }


class UnitSettings(BaseSettings):

    UNIT_KEYWORD = "unit"
    UNIT_PATTERN = [r",?.+?in[^,]+[,]?"]


class NoteSettings(BaseSettings):

    NOTE_KEYWORD = "note"
    NOTE_PATTERN = [r",?.+?:[^,]+[,]?"]
