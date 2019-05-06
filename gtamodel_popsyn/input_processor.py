import pandas as pd
import numpy as np
import gtamodel_popsyn.constants as constants
from gtamodel_popsyn._gtamodel_popsyn_processor import GTAModelPopSynProcessor
import gtamodel_popsyn.control_totals_builder as ctb
from shutil import copyfile


class InputProcessor(GTAModelPopSynProcessor):
    """
    InputProcessor will read and process the input configuration and apply and defined attribute
    mappings in the seed population records.
    """

    @property
    def processed_persons(self):
        return self._processed_persons

    @property
    def processed_households(self):
        return self._persons_households

    def __init__(self, gtamodel_popsyn_instance):
        """
        :param config: configuration input
        """
        GTAModelPopSynProcessor.__init__(self, gtamodel_popsyn_instance)
        self._persons_households = pd.DataFrame()
        self._households_base = pd.DataFrame()
        self._persons_base = pd.DataFrame()
        self._zones = pd.DataFrame()
        self._control_totals_builder = ctb.ControlTotalsBuilder(gtamodel_popsyn_instance)
        self._processed_persons = None
        self._processed_households = None

        pd.set_option('mode.chained_assignment', 'raise')

        return

    def generate(self):
        """
        Process the model input data and generate control files and
        seed records in the appropriate formats.
        :return:
        """

        # read input data
        self._read_persons_households()
        # perform control total processing here
        self._control_totals_builder.build_control_totals(self._households_base,
                                                          self._persons_households,
                                                          self._zones)

        # perform any post process modifications and write results to file
        self._post_process_persons_households()

        return

    def _read_persons_households(self):
        """
        Reads persons and households input data and joins / merges them into a single
        data frame for processing.
        :return:
        """

        # read zone information
        self._zones = pd.read_csv("data/Zones.csv",
                                  dtype={'Zone#': int, 'PD': int})[['Zone#', 'PD']]

        self._zones = self._zones.sort_values(['PD', 'Zone#']).reset_index()

        # self._zones = self._zones[self._zones.PD > 0]

        # read in csv data
        self._persons_base = pd.read_csv(f"{self._config['PersonsSeedFile']}",
                                         dtype={'HouseholdId': int})

        self._households_base = pd.read_csv(f"{self._config['HouseholdsSeedFile']}",
                                            dtype={'HouseholdZone': int, 'HouseholdId': int})

        self._households_base = pd.merge(self._households_base, self._zones,
                                         left_on="HouseholdZone", right_on="Zone#")[
            ['HouseholdId', 'DwellingType', 'NumberOfPersons', 'Vehicles',
             'IncomeClass', 'ExpansionFactor', 'HouseholdZone', 'PD']] \
            .sort_values(by=['HouseholdZone'], ascending=True).reset_index()

        # assign appropriate puma values
        self._assign_puma_values()
        self._preprocess_households()
        self._preprocess_persons()

        # make expansion factor columns unique
        self._persons_households = pd.merge(left=self._persons_base, right=self._households_base,
                                            left_on="HouseholdId",
                                            right_on="HouseholdId",
                                            how="inner")

        self._preprocess_persons_households()
        self._filter_records()

        return

    def _filter_records(self):
        """
        Filters household and persons records from invalid zones and PDs
        :return:
        """
        self._households_base = self._households_base[self._households_base.HouseholdZone.isin(constants.ZONE_RANGE)]
        self._persons_households = self._persons_households[
            self._persons_households['HouseholdZone'].isin(constants.ZONE_RANGE)]

        self._households_base = self._households_base[
            self._households_base['HouseholdZone'].isin(constants.ZONE_RANGE)]
        self._persons_households.sort_values(by=['HouseholdZone', 'PD'],
                                             ascending=True).reset_index(inplace=True)
        return

    def _assign_puma_values(self):
        """
        Assigns the associated puma assignment from PD values from predefined setting
        :return:
        """
        self._households_base['puma'] = 0
        self._zones['puma'] = 0
        for index, pd_range in enumerate(constants.PUMA_PD_RANGES):
            self._zones.loc[self._zones['PD'].isin(pd_range), 'puma'] = index + 1
            self._households_base.loc[self._households_base['PD'].isin(pd_range), 'puma'] = index + 1

    def _preprocess_persons(self):
        """
        Process any person specific attributes before the control generation stage.
        """
        self._persons_base.rename(columns={'ExpansionFactor': 'weightp'}, inplace=True)
        self._persons_base.EmploymentZone = self._persons_base.EmploymentZone.astype(int)

    def _preprocess_persons_households(self):
        """
        Preprocesses the join persons and households information. This step usually involves
        removing invalid record attributes and replacing it with a draw from a distribution
        of  its associated geography.
        :return:
        """

        self._resample_invalid_category('Occupation')
        self._resample_invalid_category('EmploymentStatus')
        self._resample_invalid_category('StudentStatus')

    def _resample_invalid_category(self, category, aggregate_column='puma', invalid_value='9'):
        """
        Resamples attributes for all records with an invalid value for the associated
        category.
        :param records:
        :param category:
        :param invalid_value:
        :return:
        """
        import numpy as np
        distributions = self._persons_households.loc[
                self._persons_households[category] != invalid_value].groupby(aggregate_column)[
                category].value_counts(
                normalize=True)

        def apply_resample(row):
            """
            Adjust the category for this row
            :param row:
            :return:
            """
            if row[category] == invalid_value:
                row[category] = np.random.choice(
                    distributions[row[aggregate_column]].index.to_list(),
                    p=distributions[row[aggregate_column]].to_list())
            return row

        self._persons_households = self._persons_households.apply(lambda x: apply_resample(x),
                                                                                        axis=1)

    def _preprocess_households(self):
        """
        Process any household specific attributes before the control generation stage.
        """
        self._households_base.PD = self._households_base.PD.astype(int)
        self._households_base.puma = self._households_base.puma.astype(int)
        self._households_base.HouseholdZone = self._households_base.HouseholdZone.astype(int)

        self._households_base.IncomeClass = self._households_base.IncomeClass.astype(int)
        self._households_base.rename(columns={'ExpansionFactor': 'weighth'}, inplace=True)
        self._households_base.IncomeClass = \
            self._households_base.IncomeClass.apply(lambda x: np.random.randint(1, 7) if 7 else x)

    def _post_process_persons_households(self):
        """
        Post process the joint set of persons and households.
        :return:
        """
        # self._persons_households = self._persons_households.loc[
        #    (self._persons_households.EmploymentStatus != '9') &
        #    (self._persons_households.Occupation != '9') & (self._persons_households.StudentStatus != '9')]

        unmatched = self._persons_households.loc[:, ('HouseholdId', 'NumberOfPersons', 'PersonNumber')].groupby(
            ['HouseholdId']).agg({'NumberOfPersons': lambda x: x.iloc[0], 'PersonNumber': 'count'})

        unmatched = unmatched.loc[unmatched['NumberOfPersons'] != unmatched['PersonNumber']]

        def adjust(group):
            group['NumberOfPersons'] = unmatched.loc[group.name, 'PersonNumber']
            group['PersonNumber'] = range(1, unmatched.loc[group.name, 'PersonNumber'] + 1)
            return group

        # compare the number of persons
        self._persons_households.loc[self._persons_households.HouseholdId.isin(unmatched.index)] = \
            self._persons_households.loc[self._persons_households.HouseholdId.isin(unmatched.index)].groupby(
                'HouseholdId').apply(lambda x: adjust(x))

        self._postprocess_persons()
        self._postprocess_households()

    def _postprocess_households(self):
        """
        Performs any post process modifications to household records and writes the results to file.
        :return:
        """
        households = self._persons_households[['HouseholdId', 'puma', 'DwellingType',
                                               'NumberOfPersons', 'Vehicles',
                                               'IncomeClass', 'weighth']].copy() \
            .drop_duplicates(['HouseholdId'])
        households.rename(columns={'weighth': 'weight'}, inplace=True)
        households.sort_values(by=['HouseholdId'], ascending=True).reset_index(inplace=True)
        households['HouseholdId'] = households['HouseholdId'].astype(int)
        households['puma'] = households['puma'].astype(int)
        self._processed_households = households.copy()
        households.to_csv(f"{self._output_path}/Inputs/{self._config['ProcessedHouseholdsSeedFile']}", index=False)

    def _postprocess_persons(self):
        """
        Performs any post process modifications to person level attributes, such as value mapping
        or filtering of invalid or unwanted records.
        :return:
        """
        persons = self._persons_households[
            ['HouseholdId', 'PersonNumber', 'puma', 'Age', 'Sex', 'License', 'TransitPass', 'EmploymentStatus',
             'Occupation', 'FreeParking', 'StudentStatus', 'EmploymentZone', 'SchoolZone', 'weightp']].copy()
        persons.rename(columns={'weightp': 'weight'}, inplace=True)

        for mapping in self._config['CategoryMapping']['Persons'].items():
            persons.loc[:, mapping[0]] = persons.loc[:, mapping[0]].map(mapping[1])

        persons.sort_values(by=['HouseholdId'], ascending=True).reset_index(inplace=True)
        persons['HouseholdId'] = persons['HouseholdId'].astype(int)
        persons['puma'] = persons['puma'].astype(int)
        self._processed_persons = persons.copy()
        self._processed_persons.to_csv(f"{self._output_path}/Inputs/{self._config['ProcessedPersonsSeedFile']}",
                                       index=False)
