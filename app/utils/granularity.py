from app.core.config import DateTimeSettings, GeographySettings
from app.utils.columns_mapping import (
    find_datetime_columns,
    find_geography_columns,
)
from app.utils.common import get_key_from_dict

datetime_settings = DateTimeSettings()
geographic_settings = GeographySettings()


async def get_granularity(columns):
    datetime_columns = await find_datetime_columns(columns)
    geographic_columns = await find_geography_columns(columns)

    datetime_columns = {
        key: value for key, value in datetime_columns.items() if value
    }
    geographic_columns = {
        key: value for key, value in geographic_columns.items() if value
    }

    sorted_datetime_columns = sorted(
        datetime_columns.items(),
        key=lambda x: get_key_from_dict(
            x[0], datetime_settings.GRANULARITY_ORDER
        ),
    )
    sorted_geography_columns = sorted(
        geographic_columns.items(),
        key=lambda x: get_key_from_dict(
            x[0], geographic_settings.GRANULARITY_ORDER
        ),
    )

    granularity_values = []
    if len(sorted_datetime_columns) > 0:
        granularity_values.append(
            datetime_settings.GRANULARITY_REPRESENTATION[
                sorted_datetime_columns[0][0]
            ]
        )

    if len(sorted_geography_columns) > 0:
        granularity_values.append(
            geographic_settings.GRANULARITY_REPRESENTATION[
                sorted_geography_columns[0][0]
            ]
        )

    return {"granularity": ", ".join(granularity_values)}
