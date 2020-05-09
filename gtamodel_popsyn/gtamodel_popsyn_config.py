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

    def __init__(self, gtamodel_popsyn_instance):
        super().__init__(gtamodel_popsyn_instance)
        self._internal_zone_range: pd.Series = pd.Series()
        self._external_zone_range: pd.Series = pd.Series()

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