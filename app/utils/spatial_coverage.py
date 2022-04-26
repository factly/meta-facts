import pandas as pd

from app.core.config import GeographySettings
from app.utils.columns_mapping import find_geography_columns

geography_settings = GeographySettings()


class GeoCoverage:
    def __init__(self, dataset, columns, geo_entity_type):
        self.dataset = dataset
        self.get_geographic_columns = columns
        self.geo_entity_type = geo_entity_type

    def nunique(self):
        return sum(
            self.dataset[list(self.get_geographic_columns)].nunique(
                dropna=True
            )
        )

    def single_unique_value(self):
        if self.nunique() > 1:
            return geography_settings.SPATIAL_COVERAGE_MULTIPLE_CONVENTION.get(
                self.geo_entity_type
            )

        unique_values_with_nan = pd.unique(
            self.dataset[self.get_geographic_columns].values.ravel()
        )
        wnique_values_without_nan = unique_values_with_nan[
            pd.notna(unique_values_with_nan)
        ]
        return wnique_values_without_nan[0]


async def get_spatial_coverage(dataset):

    geographic_columns = await find_geography_columns(dataset.columns)

    # remove all columns whose values are null
    geographic_columns = {
        key: value for key, value in geographic_columns.items() if value
    }
    if not geographic_columns:
        return {
            "spatial_coverage": geography_settings.DEFAULT_SPATIAL_COVERAGE
        }

    # get details about each geography entity present in the dataset
    geo_coverage_dict = {}
    for column_type in geographic_columns:
        column_type_geo_coverage = GeoCoverage(
            dataset, geographic_columns[column_type], column_type
        )
        geo_coverage_dict[column_type] = {
            "nunique": column_type_geo_coverage.nunique(),
            "unique_value": column_type_geo_coverage.single_unique_value(),
        }

    # if there are geographic values then arrange them is their priority order
    # Countries > States > Cities
    ordered_geographic_entity = [
        entity
        for entity in geography_settings.SPATIAL_COVERAGE_ORDER
        if entity in geo_coverage_dict.keys()
    ]
    part, whole = (
        None,
        None
        if "country" in ordered_geographic_entity
        else geography_settings.DEFAULT_SPATIAL_COVERAGE,
    )

    for entity in ordered_geographic_entity:
        if geo_coverage_dict[entity]["nunique"] > 1:
            part = geo_coverage_dict[entity]["unique_value"]
        else:
            whole = geo_coverage_dict[entity]["unique_value"]

    if not part:
        return {"spatial_coverage": whole}

    if not whole:
        return {"spatial_coverage": part}

    return {"spatial_coverage": f"{part} of {whole}"}
