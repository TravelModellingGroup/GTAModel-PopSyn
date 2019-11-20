import modin.pandas as pd

from gtamodel_popsyn._gtamodel_popsyn_processor import GTAModelPopSynProcessor
from gtamodel_popsyn.constants import *
import os


class ValidationReport(GTAModelPopSynProcessor):
    """
    Generates a set of files that provides support for validation and analysis
    of the synthetic population.

    The validation report step requires the previous' step's output files to have
    already been created.
    """

    def __init__(self, gtamodel_popsyn_instance):
        """

        :param gtamodel_popsyn_instance:
        """
        GTAModelPopSynProcessor.__init__(self, gtamodel_popsyn_instance)
        self._households_original = pd.DataFrame()
        self._persons_original = pd.DataFrame()
        self._persons_households_original = pd.DataFrame()
        self._meta_control_totals = pd.DataFrame()
        self._households_synthesized = pd.DataFrame()
        self._persons_synthesized = pd.DataFrame()
        self._persons_households_synthesized = pd.DataFrame()
        self._zones = pd.DataFrame()

    def generate(self):
        """
        Starts the validation report generation process.
        :return:
        """
        try:
            os.makedirs(f'{self._output_folder}/Validation/')
        except FileExistsError:
            pass
        self._read_data()
        self._process()

    def _read_data(self):
        """
        Reads in all output data
        :return:
        """
        self._households_original = pd.read_csv(f"{self._config['HouseholdsSeedFile']}")
        self._persons_original = pd.read_csv(f"{self._config['PersonsSeedFile']}")
        self._meta_control_totals = pd.read_csv(f"{self._output_path}/Inputs/{self._config['MetaLevelControls']}")
        self._households_synthesized = pd.read_csv(
            f'{self._output_path}/{self._config["HouseholdsOutputFile"]}')
        self._persons_synthesized = pd.read_csv(
            f'{self._output_path}/{self._config["PersonsOutputFile"]}')

        self._zones = pd.read_csv(self._config['Zones'])

    def _process(self):
        self._zones = self._zones[['Zone#', 'PD']].rename(columns={'Zone#': 'HouseholdZone'})

        self._persons_synthesized['EmploymentZone'] = self._persons_synthesized['EmploymentZone'].astype(int)
        self._persons_original['EmploymentZone'] = self._persons_original['EmploymentZone'].astype(int)

        self._persons_households_original = pd.merge(self._persons_original, self._households_original,
                                                     left_on="HouseholdId", right_on="HouseholdId")
        self._persons_households_original = pd.merge(self._persons_households_original, self._zones,
                                                     left_on="HouseholdZone", right_on="HouseholdZone")

        self._persons_households_synthesized = pd.merge(self._persons_synthesized, self._households_synthesized,
                                                        left_on="HouseholdId", right_on="HouseholdId")
        self._persons_households_synthesized = pd.merge(self._persons_households_synthesized, self._zones,
                                                        left_on="HouseholdZone", right_on="HouseholdZone")

        self._persons_households_synthesized = self._persons_households_synthesized[
            self._persons_households_synthesized['PD'] > 0]
        self._persons_households_original = self._persons_households_original[
            self._persons_households_original['PD'] > 0]

        self._persons_households_synthesized = self._persons_households_synthesized[
            self._persons_households_synthesized['HouseholdZone'] < ZONE_RANGE.stop]
        self._persons_households_original = self._persons_households_original[
            self._persons_households_original['HouseholdZone'] < ZONE_RANGE.stop]

        self._households_original = self._households_original[
            self._households_original['HouseholdZone'] < ZONE_RANGE.stop]

        self._households_synthesized = self._households_synthesized[
            self._households_synthesized['HouseholdZone'] < ZONE_RANGE.stop]

        self._persons_households_synthesized = self._persons_households_synthesized.rename(
            columns={'ExpansionFactor_x': 'ExpansionFactor'})
        self._persons_households_original = self._persons_households_original.rename(
            columns={'ExpansionFactor_x': 'ExpansionFactor'})

        # self._process_occupation_employment_zone()

        self._process_persons_totals()
        self._process_household_totals()
        # self._process_occupation_employment_zone()

    def _process_persons_totals(self):
        """
        Processes and output region totals for person level attributes, and write the comparison to file.
        :return:
        """

        totals = pd.DataFrame(columns=['Observed Total', 'Synthesized Total', 'Abs. Difference'],
                              index=['Population', 'Male', 'Female',
                                     'Occupation P', 'Occupation G', 'Occupation S', 'Occupation M', 'Occupation O'])

        totals.loc['Population', 'Observed Total'] = self._persons_households_original['ExpansionFactor'].sum()
        totals.loc['Population', 'Synthesized Total'] = self._persons_households_synthesized['ExpansionFactor'].sum()

        totals.loc['Male', 'Observed Total'] = self._persons_households_original.loc[
            self._persons_households_original.Sex == 'M', 'ExpansionFactor'].sum()
        totals.loc['Male', 'Synthesized Total'] = self._persons_households_synthesized.loc[
            self._persons_households_synthesized.Sex == 'M', 'ExpansionFactor'].sum()

        totals.loc['Female', 'Observed Total'] = self._persons_households_original.loc[
            self._persons_households_original.Sex == 'F', 'ExpansionFactor'].sum()
        totals.loc['Female', 'Synthesized Total'] = self._persons_households_synthesized.loc[
            self._persons_households_synthesized.Sex == 'F', 'ExpansionFactor'].sum()

        totals.loc['Occupation P', 'Observed Total'] = self._persons_households_original.loc[
            self._persons_households_original.Occupation == 'P', 'ExpansionFactor'].sum()
        totals.loc['Occupation P', 'Synthesized Total'] = self._persons_households_synthesized.loc[
            self._persons_households_synthesized.Occupation == 'P', 'ExpansionFactor'].sum()

        totals.loc['Occupation G', 'Observed Total'] = self._persons_households_original.loc[
            self._persons_households_original.Occupation == 'G', 'ExpansionFactor'].sum()
        totals.loc['Occupation G', 'Synthesized Total'] = self._persons_households_synthesized.loc[
            self._persons_households_synthesized.Occupation == 'G', 'ExpansionFactor'].sum()

        totals.loc['Occupation S', 'Observed Total'] = self._persons_households_original.loc[
            self._persons_households_original.Occupation == 'S', 'ExpansionFactor'].sum()
        totals.loc['Occupation S', 'Synthesized Total'] = self._persons_households_synthesized.loc[
            self._persons_households_synthesized.Occupation == 'S', 'ExpansionFactor'].sum()

        totals.loc['Occupation M', 'Observed Total'] = self._persons_households_original.loc[
            self._persons_households_original.Occupation == 'M', 'ExpansionFactor'].sum()
        totals.loc['Occupation M', 'Synthesized Total'] = self._persons_households_synthesized.loc[
            self._persons_households_synthesized.Occupation == 'M', 'ExpansionFactor'].sum()

        totals.loc['Occupation O', 'Observed Total'] = self._persons_households_original.loc[
            self._persons_households_original.Occupation == 'O', 'ExpansionFactor'].sum()
        totals.loc['Occupation O', 'Synthesized Total'] = self._persons_households_synthesized.loc[
            self._persons_households_synthesized.Occupation == 'O', 'ExpansionFactor'].sum()

        totals.loc['EmploymentStatus F', 'Observed Total'] = self._persons_households_original.loc[
            self._persons_households_original.EmploymentStatus == 'F', 'ExpansionFactor'].sum()
        totals.loc['EmploymentStatus F', 'Synthesized Total'] = self._persons_households_synthesized.loc[
            self._persons_households_synthesized.EmploymentStatus == 'F', 'ExpansionFactor'].sum()

        totals.loc['EmploymentStatus P', 'Observed Total'] = self._persons_households_original.loc[
            self._persons_households_original.EmploymentStatus == 'P', 'ExpansionFactor'].sum()
        totals.loc['EmploymentStatus P', 'Synthesized Total'] = self._persons_households_synthesized.loc[
            self._persons_households_synthesized.EmploymentStatus == 'P', 'ExpansionFactor'].sum()

        totals.loc['EmploymentStatus O', 'Observed Total'] = self._persons_households_original.loc[
            self._persons_households_original.EmploymentStatus == 'O', 'ExpansionFactor'].sum()
        totals.loc['EmploymentStatus O', 'Synthesized Total'] = self._persons_households_synthesized.loc[
            self._persons_households_synthesized.EmploymentStatus == 'O', 'ExpansionFactor'].sum()

        totals.loc['EmploymentStatus H', 'Observed Total'] = self._persons_households_original.loc[
            self._persons_households_original.EmploymentStatus == 'H', 'ExpansionFactor'].sum()
        totals.loc['EmploymentStatus H', 'Synthesized Total'] = self._persons_households_synthesized.loc[
            self._persons_households_synthesized.EmploymentStatus == 'H', 'ExpansionFactor'].sum()

        totals.loc['EmploymentStatus J', 'Observed Total'] = self._persons_households_original.loc[
            self._persons_households_original.EmploymentStatus == 'J', 'ExpansionFactor'].sum()
        totals.loc['EmploymentStatus J', 'Synthesized Total'] = self._persons_households_synthesized.loc[
            self._persons_households_synthesized.EmploymentStatus == 'J', 'ExpansionFactor'].sum()

        totals.loc['EmploymentZone Internal', 'Observed Total'] = self._persons_households_original.loc[
            self._persons_households_original.EmploymentZone < INTERNAL_ZONE_RANGE.stop, 'ExpansionFactor'].sum()
        totals.loc['EmploymentZone Internal', 'Synthesized Total'] = self._persons_households_synthesized.loc[
            self._persons_households_synthesized.EmploymentZone < INTERNAL_ZONE_RANGE.stop, 'ExpansionFactor'].sum()

        totals.loc['EmploymentZone Roaming', 'Observed Total'] = self._persons_households_original.loc[
            self._persons_households_original.EmploymentZone == ROAMING_ZONE_ID, 'ExpansionFactor'].sum()
        totals.loc['EmploymentZone Roaming', 'Synthesized Total'] = self._persons_households_synthesized.loc[
            self._persons_households_synthesized.EmploymentZone == ROAMING_ZONE_ID, 'ExpansionFactor'].sum()

        totals.loc['EmploymentZone External', 'Observed Total'] = self._persons_households_original.loc[
            (self._persons_households_original.EmploymentZone >= EXTERNAL_ZONE_RANGE.start) &
            (self._persons_households_original.EmploymentZone <= EXTERNAL_ZONE_RANGE.stop), 'ExpansionFactor'].sum()

        totals.loc['EmploymentZone External', 'Synthesized Total'] = self._persons_households_synthesized.loc[
            (self._persons_households_synthesized.EmploymentZone >= EXTERNAL_ZONE_RANGE.start) &
            (self._persons_households_synthesized.EmploymentZone <= EXTERNAL_ZONE_RANGE.stop), 'ExpansionFactor'].sum()

        totals.loc['StudentStatus S', 'Observed Total'] = self._persons_households_original.loc[
            self._persons_households_original.StudentStatus == 'S', 'ExpansionFactor'].sum()
        totals.loc['StudentStatus S', 'Synthesized Total'] = self._persons_households_synthesized.loc[
            self._persons_households_synthesized.StudentStatus == 'S', 'ExpansionFactor'].sum()

        totals.loc['StudentStatus P', 'Observed Total'] = self._persons_households_original.loc[
            self._persons_households_original.StudentStatus == 'P', 'ExpansionFactor'].sum()
        totals.loc['StudentStatus P', 'Synthesized Total'] = self._persons_households_synthesized.loc[
            self._persons_households_synthesized.StudentStatus == 'P', 'ExpansionFactor'].sum()

        totals.loc['StudentStatus O', 'Observed Total'] = self._persons_households_original.loc[
            self._persons_households_original.StudentStatus == 'O', 'ExpansionFactor'].sum()
        totals.loc['StudentStatus O', 'Synthesized Total'] = self._persons_households_synthesized.loc[
            self._persons_households_synthesized.StudentStatus == 'O', 'ExpansionFactor'].sum()



        for bin in AGE_BINS:
            totals.loc[f'Age {bin.start} - {bin.stop}', 'Observed Total'] = self._persons_households_original.loc[
                (self._persons_households_original.Age >= bin.start) & (
                            self._persons_households_original.Age <= bin.stop), 'ExpansionFactor'].sum()
            totals.loc[f'Age {bin.start} - {bin.stop}', 'Synthesized Total'] = self._persons_households_synthesized.loc[
                (self._persons_households_synthesized.Age >= bin.start) & (
                        self._persons_households_synthesized.Age <= bin.stop), 'ExpansionFactor'].sum()

        totals['Abs. Difference'] = totals['Observed Total'] - totals['Synthesized Total']
        totals.to_csv(f'{self._output_path}/Validation/persons_totals.csv', index=True)

    def _process_household_totals(self):
        """
        Aggregates and compares all household attributes at the PD level

        :return:
        """
        totals = pd.DataFrame(columns=['Observed Total', 'Synthesized Total', 'Abs. Difference'])

        totals.loc['Total Households', 'Observed Total'] = self._households_original['ExpansionFactor'].sum()
        totals.loc['Total Households', 'Synthesized Total'] = self._households_synthesized['ExpansionFactor'].sum()

        totals.loc['Income Class 1', 'Observed Total'] = self._households_original.loc[
            self._households_original.IncomeClass == 1, 'ExpansionFactor'].sum()
        totals.loc['Income Class 1', 'Synthesized Total'] = self._households_synthesized.loc[
            self._households_synthesized.IncomeClass == 1, 'ExpansionFactor'].sum()

        totals.loc['Income Class 2', 'Observed Total'] = self._households_original.loc[
            self._households_original.IncomeClass == 2, 'ExpansionFactor'].sum()
        totals.loc['Income Class 2', 'Synthesized Total'] = self._households_synthesized.loc[
            self._households_synthesized.IncomeClass == 2, 'ExpansionFactor'].sum()

        totals.loc['Income Class 3', 'Observed Total'] = self._households_original.loc[
            self._households_original.IncomeClass == 3, 'ExpansionFactor'].sum()
        totals.loc['Income Class 3', 'Synthesized Total'] = self._households_synthesized.loc[
            self._households_synthesized.IncomeClass == 3, 'ExpansionFactor'].sum()

        totals.loc['Income Class 4', 'Observed Total'] = self._households_original.loc[
            self._households_original.IncomeClass == 4, 'ExpansionFactor'].sum()
        totals.loc['Income Class 4', 'Synthesized Total'] = self._households_synthesized.loc[
            self._households_synthesized.IncomeClass == 4, 'ExpansionFactor'].sum()

        totals.loc['Income Class 5', 'Observed Total'] = self._households_original.loc[
            self._households_original.IncomeClass == 5, 'ExpansionFactor'].sum()
        totals.loc['Income Class 5', 'Synthesized Total'] = self._households_synthesized.loc[
            self._households_synthesized.IncomeClass == 5, 'ExpansionFactor'].sum()

        totals.loc['Income Class 6', 'Observed Total'] = self._households_original.loc[
            self._households_original.IncomeClass == 6, 'ExpansionFactor'].sum()
        totals.loc['Income Class 6', 'Synthesized Total'] = self._households_synthesized.loc[
            self._households_synthesized.IncomeClass == 6, 'ExpansionFactor'].sum()

        totals.loc['Income Class 7', 'Observed Total'] = self._households_original.loc[
            self._households_original.IncomeClass == 7, 'ExpansionFactor'].sum()
        totals.loc['Income Class 7', 'Synthesized Total'] = self._households_synthesized.loc[
            self._households_synthesized.IncomeClass == 7, 'ExpansionFactor'].sum()

        totals.loc['Number of Persons 1', 'Observed Total'] = self._households_original.loc[
            self._households_original.NumberOfPersons == 1, 'ExpansionFactor'].sum()
        totals.loc['Number of Persons 1', 'Synthesized Total'] = self._households_synthesized.loc[
            self._households_synthesized.NumberOfPersons == 1, 'ExpansionFactor'].sum()

        totals.loc['Number of Persons 2', 'Observed Total'] = self._households_original.loc[
            self._households_original.NumberOfPersons == 2, 'ExpansionFactor'].sum()
        totals.loc['Number of Persons 2', 'Synthesized Total'] = self._households_synthesized.loc[
            self._households_synthesized.NumberOfPersons == 2, 'ExpansionFactor'].sum()

        totals.loc['Number of Persons 3', 'Observed Total'] = self._households_original.loc[
            self._households_original.NumberOfPersons == 3, 'ExpansionFactor'].sum()
        totals.loc['Number of Persons 3', 'Synthesized Total'] = self._households_synthesized.loc[
            self._households_synthesized.NumberOfPersons == 3, 'ExpansionFactor'].sum()

        totals.loc['Number of Persons 4+', 'Observed Total'] = self._households_original.loc[
            self._households_original.NumberOfPersons >= 4, 'ExpansionFactor'].sum()
        totals.loc['Number of Persons 4+', 'Synthesized Total'] = self._households_synthesized.loc[
            self._households_synthesized.NumberOfPersons >= 4, 'ExpansionFactor'].sum()

        totals['Abs. Difference'] = totals['Observed Total'] - totals['Synthesized Total']
        totals.to_csv(f'{self._output_path}/Validation/households_totals.csv', index=True)

    def _process_occupation_employment_zone(self):
        """

        :param group_original:
        :param group_synthesized:
        :return:
        """

        self._persons_households_original['EmploymentZoneType'] = self._persons_households_original.apply(
            lambda x: ('Roaming' if x['EmploymentZone'] == ROAMING_ZONE_ID
                       else (
                'Internal' if x['EmploymentZone'] < ZONE_RANGE.start else 'External')), axis=1)

        self._persons_households_synthesized['EmploymentZoneType'] = self._persons_households_synthesized.apply(
            lambda x: ('Roaming' if x['EmploymentZone'] == ROAMING_ZONE_ID
                       else (
                'Internal' if x['EmploymentZone'] < ZONE_RANGE.start else 'External')), axis=1)

        group_original = self._persons_households_original.groupby(
            ['PD', 'Occupation', 'EmploymentStatus', 'EmploymentZoneType'])[
            'Total Input'].sum()
        group_synthesized = self._persons_households_synthesized.groupby(
            ['PD', 'Occupation', 'EmploymentStatus', 'EmploymentZoneType'])[
            'Total Synthesized'].sum()

        persons_households_combined = pd.concat([group_original, group_synthesized], axis=1)
        persons_households_combined = persons_households_combined.fillna(0)
        persons_households_combined['Delta'] = persons_households_combined['Total Synthesized'] - \
                                               persons_households_combined['Total Input']

        persons_households_combined.loc[range(1, 60), ['P', 'G', 'S', 'M', 'O'], ['F', 'P', 'O', 'J', 'H']].to_csv(
            f'{self._config["OutputFolder"]}/Validation/pd_occupation_employment_status_zone.csv')
        return
