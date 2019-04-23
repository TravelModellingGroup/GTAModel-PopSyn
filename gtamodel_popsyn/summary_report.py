import pandas as pd
from gtamodel_popsyn.constants import *


class SummaryReport(object):
    """
    Generates a summary report comparing attribute totals between the input records
    and the synthetic population
    """

    def __init__(self, config):
        """
        """
        self._config = config
        self._households_original = pd.DataFrame()
        self._persons_original = pd.DataFrame()
        self._persons_households_original = pd.DataFrame()

        self._households_synthesized = pd.DataFrame()
        self._persons_synthesized = pd.DataFrame()
        self._persons_households_synthesized = pd.DataFrame()
        self._zones = pd.DataFrame()

    def generate(self):
        """

        :return:
        """
        self._read_data()
        self._process()

    def _read_data(self):
        self._households_original = pd.read_csv(f"{self._config['HouseholdsSeedFile']}")
        self._persons_original = pd.read_csv(f"{self._config['PersonsSeedFile']}")

        self._households_synthesized = pd.read_csv(
            f'{self._config["OutputFolder"]}/{self._config["HouseholdsOutputFile"]}')
        self._persons_synthesized = pd.read_csv(
            f'{self._config["OutputFolder"]}/{self._config["PersonsOutputFile"]}')

        self._zones = pd.read_csv('data/Zones.csv')

    def _process(self):
        self._zones = self._zones[['Zone#', 'PD']].rename(columns={'Zone#': 'HouseholdZone'})
        self._zones = self._zones[self._zones['PD'] > 0]

        self._persons_synthesized['EmploymentZone'] = self._persons_synthesized['EmploymentZone'].astype(int)
        self._persons_original['EmploymentZone'] = self._persons_synthesized['EmploymentZone'].astype(int)

        self._persons_households_original = pd.merge(self._persons_original, self._households_original,
                                                     left_on="HouseholdId", right_on="HouseholdId")
        self._persons_households_original = pd.merge(self._persons_households_original, self._zones,
                                                     left_on="HouseholdZone", right_on="HouseholdZone")

        self._persons_households_synthesized = pd.merge(self._persons_synthesized, self._households_synthesized,
                                                        left_on="HouseholdId", right_on="HouseholdId")
        self._persons_households_synthesized = pd.merge(self._persons_households_synthesized, self._zones,
                                                        left_on="HouseholdZone", right_on="HouseholdZone")

        self._persons_households_synthesized = self._persons_households_synthesized.rename(
            columns={'ExpansionFactor_x': 'Total_Synthesized'})
        self._persons_households_original = self._persons_households_original.rename(
            columns={'ExpansionFactor_x': 'Total_Input'})


        self._process_occupation_employment_zone()

    def _process_occupation_employment_zone(self):
        """

        :param group_original:
        :param group_synthesized:
        :return:
        """

        self._persons_households_original[
            'EmploymentZoneType']  = self._persons_households_original.apply(
            lambda x: ('Roaming' if x['EmploymentZone'] == 8888
                       else (
                'Internal' if x['EmploymentZone'] < ZONE_RANGE.start else 'External')), axis=1)

        self._persons_households_synthesized['EmploymentZoneType'] = self._persons_households_synthesized.apply(
            lambda x: ('Roaming' if x['EmploymentZone'] == 8888
                       else (
                'Internal' if x['EmploymentZone'] < ZONE_RANGE.start else 'External')), axis=1)

        group_original = self._persons_households_original.groupby(
            ['PD', 'Occupation', 'EmploymentStatus', 'EmploymentZoneType'])[
            'Total_Input'].sum()
        group_synthesized = self._persons_households_synthesized.groupby(
            ['PD', 'Occupation', 'EmploymentStatus', 'EmploymentZoneType'])[
            'Total_Synthesized'].sum()


        persons_households_combined = pd.concat([group_original, group_synthesized], axis=1)
        persons_households_combined['Delta'] = persons_households_combined['Total_Synthesized'] - persons_households_combined['Total_Input']

        persons_households_combined.loc[range(1,60),['P','G','S','M','O'],['F','P','O','J','H']].to_csv('temp/ph_combined.csv')
        return
