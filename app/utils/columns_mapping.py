import re
from itertools import chain

from app.core.config import (
    DateTimeSettings,
    GeographySettings,
    NoteSettings,
    OtherSettings,
    UnitSettings,
)

datetime_settings = DateTimeSettings()
geography_settings = GeographySettings()
unit_settings = UnitSettings()
note_settings = NoteSettings()
other_settings = OtherSettings()


def extract_pattern_from_columns(
    columns: set,
    pattern,
):
    matched_columns = set(filter(pattern.match, columns))
    return matched_columns, columns.difference(matched_columns)


async def find_other_granular_columns(columns: set):
    airline_pattern = re.compile(
        r".*({})".format(other_settings.AIRLINE_KEYWORD)
    )
    airport_pattern = re.compile(
        r".*({})".format(other_settings.AIRPORT_KEYWORD)
    )
    language_pattern = re.compile(
        r".*({})".format(other_settings.LANGUAGE_KEYWORD)
    )
    crop_pattern = re.compile(r".*({})".format(other_settings.CROPS_KEYWORD))
    gender_pattern = re.compile(
        r".*({})".format(other_settings.AIRPORT_KEYWORD)
    )

    airline_columns, columns = extract_pattern_from_columns(
        columns, airline_pattern
    )
    airport_columns, columns = extract_pattern_from_columns(
        columns, airport_pattern
    )
    language_columns, columns = extract_pattern_from_columns(
        columns, language_pattern
    )
    crop_columns, columns = extract_pattern_from_columns(columns, crop_pattern)
    gender_columns, columns = extract_pattern_from_columns(
        columns, gender_pattern
    )

    return {
        "airline": airline_columns,
        "airport": airport_columns,
        "language": language_columns,
        "crop": crop_columns,
        "gender": gender_columns,
    }


async def find_datetime_columns(columns: set):
    fiscal_year_pattern = re.compile(
        r".*({})".format(
            datetime_settings.FISCAL_YEAR_KEYWORD,
        )
    )
    academic_year_pattern = re.compile(
        r".*({})".format(
            datetime_settings.ACADEMIC_YEAR_KEYWORD,
        )
    )
    cal_year_pattern = re.compile(
        r".*({})".format(datetime_settings.CALENDAR_YEAR_KEYWORD)
    )
    other_year_pattern = re.compile(
        r".*({})".format(datetime_settings.OTHER_YEAR_KEYWORD)
    )
    quarter_pattern = re.compile(
        r".*({})".format(datetime_settings.QUARTER_KEYWORD)
    )
    month_pattern = re.compile(
        r".*({})".format(datetime_settings.MONTH_KEYWORD)
    )
    date_pattern = re.compile(r"^.*(?:^|_){}s?(?:_|$)|^.*(?:^|_){}(?:_|$)".format(datetime_settings.DATE_KEYWORD, datetime_settings.DATE_KEYWORD))
    as_on_date_pattern = re.compile(
        r".*({})".format(datetime_settings.AS_ON_DATE_PATTERN)
    )

    fiscal_year_columns, columns = extract_pattern_from_columns(
        columns, fiscal_year_pattern
    )
    academic_year_columns, columns = extract_pattern_from_columns(
        columns, academic_year_pattern
    )
    year_columns, columns = extract_pattern_from_columns(
        columns, cal_year_pattern
    )
    other_year_columns, columns = extract_pattern_from_columns(
        columns, other_year_pattern
    )
    quarter_columns, columns = extract_pattern_from_columns(
        columns, quarter_pattern
    )
    month_columns, columns = extract_pattern_from_columns(
        columns, month_pattern
    )
    date_columns, columns = extract_pattern_from_columns(columns, date_pattern)

    # filter out `as_on_date` from date columns
    date_columns = {
        col for col in date_columns if not as_on_date_pattern.match(col)
    }
    return {
        "fiscal_year": fiscal_year_columns,
        "academic_year": academic_year_columns,
        "calender_year": year_columns,
        "other_year": other_year_columns,
        "quarter": quarter_columns,
        "month": month_columns,
        "date": date_columns,
    }


async def find_geography_columns(columns: set):
    country_pattern = re.compile(
        r".*({})".format(geography_settings.COUNTRY_KEYWORD)
    )
    state_pattern = re.compile(
        r".*({})".format(geography_settings.STATE_KEYWORD)
    )
    district_pattern = re.compile(
        r".*({})".format(geography_settings.DISTRICT_KEYWORD)
    )

    country_column, columns = extract_pattern_from_columns(
        columns, country_pattern
    )
    state_columns, columns = extract_pattern_from_columns(
        columns, state_pattern
    )
    district_columns, columns = extract_pattern_from_columns(
        columns, district_pattern
    )

    return {
        "country": country_column,
        "state": state_columns,
        "district": district_columns,
    }


async def find_unit_columns(columns: set):
    unit_pattern = re.compile(r"({})".format(unit_settings.UNIT_KEYWORD))
    unit_column, _ = extract_pattern_from_columns(columns, unit_pattern)
    return {
        "unit": unit_column,
    }


async def find_note_columns(columns: set):
    note_pattern = re.compile(r"({})".format(note_settings.NOTE_KEYWORD))
    note_column, _ = extract_pattern_from_columns(columns, note_pattern)
    return {
        "note": note_column,
    }


async def find_object_columns(dataset):
    object_columns = list(dataset.select_dtypes(include=["object"]).columns)
    return {"object_columns": object_columns}


async def find_mapped_columns(columns):
    datetime_columns = await find_datetime_columns(columns)
    geography_columns = await find_geography_columns(columns)
    unit_columns = await find_unit_columns(columns)
    note_columns = await find_note_columns(columns)
    mapped_columns = {
        **datetime_columns,
        **geography_columns,
        **unit_columns,
        **note_columns,
    }

    not_mapped_columns = list(
        set(columns).difference(
            list(chain.from_iterable(mapped_columns.values()))
        )
    )

    return {**mapped_columns, "unmapped": not_mapped_columns}
