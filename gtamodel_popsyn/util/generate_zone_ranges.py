import pandas as pd


def generate_zone_ranges(zone_ranges: list) -> pd.Series:
    """
    Generates a series from a nested list of range extremes. Each pairwise
    list within the outer list represents the left and right side of a range.
    The ranges are then combined into a single series.
    @param zone_ranges: The list of range values
    @return: A pandas series with the values falling between the defined ranges
    """
    zone_range = pd.Series()
    for r in zone_ranges:
        series_range = pd.Series(range(r[0], r[1] + 1))
        zone_range = zone_range.append(series_range)
    return zone_range
