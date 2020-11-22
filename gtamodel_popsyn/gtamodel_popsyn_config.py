import json

from gtamodel_popsyn._gtamodel_popsyn_processor import GTAModelPopSynProcessor
from gtamodel_popsyn.constants import INTERNAL_ZONE_RANGE, EXTERNAL_ZONE_RANGE
from gtamodel_popsyn.util.generate_zone_ranges import generate_zone_ranges
import pandas as pd


class GTAModelPopSynConfig(GTAModelPopSynProcessor):
    """
    General configuration class for sharing information between sub components, with post processed data
    available from config and elsewhere.
    """

    @property
    def internal_zone_range(self):
        return self._internal_zone_range

    @property
    def external_zone_range(self):
        return self._external_zone_range

    @property
    def total_population_column_name(self):
        return self._total_population_column_name

    @property
    def total_households_column_name(self):
        return self._total_households_column_name

    @property
    def household_control_columns(self):
        return self._household_control_columns

    @property
    def person_control_columns(self):
        return self._person_control_columns

    @property
    def zone_pd_map(self):
        return self._zones

    def __init__(self, gtamodel_popsyn_instance):
        super().__init__(gtamodel_popsyn_instance)
        self._internal_zone_range: pd.Series = pd.Series()
        self._external_zone_range: pd.Series = pd.Series()
        self._total_population_column_name = self._config.get('TotalPopulationColumnName', False) or 'totpop'
        self._total_households_column_name = self._config.get('TotalHouseholdsColumnName', False) or 'totalhh'

        self._household_control_columns = self._config.get('HouseholdControlColumns', False) or ['totalhh']
        self._person_control_columns = self._config.get('PersonControlColumns', False) or ['totpop']
        self._zones = pd.DataFrame()

        self._process_zone_map()

    def initialize(self):
        """

        @return:
        """
        zone_ranges = []
        if 'ZoneRanges' not in self._config:
            zone_ranges.append(INTERNAL_ZONE_RANGE)
        else:
            zone_ranges = self._config['ZoneRanges']
        self._internal_zone_range = generate_zone_ranges(zone_ranges)

        external_zone_ranges = []
        if 'ExternalZoneRanges' not in self._config:
            external_zone_ranges.append(EXTERNAL_ZONE_RANGE)
        else:
            external_zone_ranges = self._config['ExternalZoneRanges']

        self._external_zone_range = generate_zone_ranges(external_zone_ranges)

    def _process_zone_map(self):
        """

        @return:
        """
        self._zones = pd.read_csv(self._config['Zones'],
                                  dtype={'Zone': int, 'PD': int})[['Zone', 'PD']]

        self._zones = self._zones.sort_values(['PD', 'Zone']).reset_index()
        self._zones['zone_idx'] = self._zones['Zone']
        self._zones.set_index('zone_idx', inplace=True)
        return
        # zone_list = self._zones['Zone'].to_list()
        # missing_zones = self._find_missing(zone_list)

        # extend the zone list with the missing ids
        # zone_list.extend(missing_zones)

        # sort ids
        # zone_list.sort()
