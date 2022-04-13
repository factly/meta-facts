import re
from itertools import chain
from typing import List

from app.core.config import DateTimeSettings

datetime_settings = DateTimeSettings()


def convert_to_calender_year(other_year):
    # input pattern : 2020-21
    # output values : 2020, 2021
    start_year = int(other_year.split("-")[0])
    next_year = start_year + 1
    return start_year, next_year


def convert_to_fiscal_year(calender_year):
    # input pattern : 2020
    # output value : 2020-21
    start_year = calender_year
    # pass the year to get the next year
    if isinstance(calender_year, str):
        start_year = int(calender_year)

    next_year = start_year + 1
    return f"{start_year}-{str(next_year)[-2:]}"


def verify_proper_format_of_year_values(
    list_of_values: List, year_pattern=datetime_settings.ALL_YEAR_FORMATS
):
    year_regex_pattern = re.compile(year_pattern)
    return all(
        bool(year_regex_pattern.match(year_val)) for year_val in list_of_values
    )


def get_list_of_years_in_interval(year_period):
    # input of 2012-13 : should give output: [2012,2013]
    start_year, ending_part = (
        int(year_period.split("-")[0]),
        year_period.split("-")[-1][-2:],
    )
    year = start_year
    domain = []
    while True:
        domain.append(year)
        if str(year)[-2:] == ending_part:
            break
        year += 1
    return domain


def get_list_mappings(unique_years):
    year_mapping = {
        unique_year: get_list_of_years_in_interval(unique_year)
        for unique_year in unique_years
    }
    return year_mapping


def is_sequence(year_mapping):
    combine_all_years = sorted(list(set(chain(*year_mapping.values()))))
    min_val = min(combine_all_years)
    max_val = max(combine_all_years)
    # check if its a discrete or continuous combine_all_years
    if combine_all_years == list(range(min_val, max_val + 1)):
        is_sequence = True
    else:
        is_sequence = False
    return is_sequence


def temporal_coverage_representation(is_sequence, year_mapping):
    year_values_from_mapping = sorted(year_mapping.keys())

    if len(year_values_from_mapping) == 1:
        return f"{year_values_from_mapping[0]}"

    if not is_sequence:
        return ", ".join(str(year) for year in year_values_from_mapping)

    return f"{year_values_from_mapping[0]} to {year_values_from_mapping[-1]}"


async def get_temporal_coverage(dataset, mapped_columns: dict):
    year_columns = list(mapped_columns["calender_year"]) + list(
        mapped_columns["non_calendar_year"]
    )
    year_columns = [year_column for year_column in year_columns if year_column]

    # do operation on the first year column
    if len(year_columns) == 0:
        return {"temporal_coverage": ""}

    year_column = year_columns[0]
    unique_year_values = [
        f"{year_val}" for year_val in dataset[year_column].unique() if year_val
    ]

    if not verify_proper_format_of_year_values(unique_year_values):
        return {"temporal_coverage": ""}

    year_mapping = get_list_mappings(unique_year_values)

    year_in_sequence = is_sequence(year_mapping)

    temporal_coverage = temporal_coverage_representation(
        year_in_sequence, year_mapping
    )
    return {"temporal_coverage": temporal_coverage}
