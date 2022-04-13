import re
from pathlib import Path

from pandas import DataFrame, read_csv

STANDARD_YEAR_COLUMN_NAME = {"year", "fiscal_year", "academic_year"}
YEAR_REGEX_PATTERN = re.compile(r"^\d{4}$")
OTHER_YEAR_REGEX_PATTERN = re.compile(r"^\d{4}-\d{2}$")


async def get_dataset(dataset_path):
    # currently get dataset sample from url
    dataset = read_csv(dataset_path)
    return dataset


async def get_files_from_directory(directory: str):
    # funcion currently asumes that local path is proivided
    # ? should this method will look if dataset in local path and remote path as well
    directory_path = Path(directory)
    if not directory_path.exists():
        raise ValueError(f"Directory {directory} does not exist")

    # if directory is present take all dataset within that directory or subdirectories
    # for each_path in directory_path.rglob('**/*.csv'):
    #     required_path = str(each_path).split("/projects/")[-1]
    #     yield required_path
    return [str(each_path) for each_path in directory_path.rglob("**/*.csv")]


class ColumnAttributes:
    def __init__(self, dataset: DataFrame) -> None:
        self.dataset = dataset

    def year_column(self):
        # There are 3 different year column with 2 difffernet formats
        # year, fiscal_year, academic_year
        year_columns_in_datasets = [
            col
            for col in self.dataset.columns
            if col in STANDARD_YEAR_COLUMN_NAME
        ]
        if len(year_columns_in_datasets) == 0:
            return False
        return year_columns_in_datasets[0]


# Transform irregular 2D list into a regular one.
def flatten_irregular_nested_list(nested_list):
    regular_list = []
    for ele in nested_list:
        if type(ele) is list:
            regular_list.append(ele.strip())
        elif type(ele) is str:
            regular_list.append([ele.strip()])
    return regular_list


def get_key_from_dict(val, my_dict):
    for key, value in my_dict.items():
        if val in value:
            return key
