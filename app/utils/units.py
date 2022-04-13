from itertools import chain

unit_columns = ["unit"]


async def get_units(dataset, mapped_columns):
    # if there are no unique unit columns present then return empty dict
    unit_columns = mapped_columns.get("unit", [])
    unit_columns = list(unit_columns)
    if len(unit_columns) != 1:
        return {"units": ""}

    # if distinguished unit column is present then return its value
    # find all unique set of units presnt in a dataset
    unit_column_unique_values = [
        unit.split(",")
        for unit in dataset[unit_columns[0]].unique()
        if isinstance(unit, str)
    ]
    unique_units_from_unit_columns = list(
        chain.from_iterable(unit_column_unique_values)
    )

    return {"units": ", ".join(unique_units_from_unit_columns)}
