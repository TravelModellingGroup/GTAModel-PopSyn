import pandas as pd
import numpy as np
import gtamodel_popsyn.control_totals_builder as gtactb


class InputProcessor(object):
    """
    InputProcessor will read and process the input configuration and apply and defined attribute
    mappings in the seed population records.
    """

    # PD -> Puma mappings
    _PUMA_PD_RANGES = [range(1, 17), range(17, 10000)]
    _ZONE_RANGE = range(1, 6000)

    def __init__(self, config):
        self._config = config
        self._persons_households = pd.DataFrame()
        self._households_base = pd.DataFrame()
        self._persons_base = pd.DataFrame()
        self._zones = pd.DataFrame()
        self._control_totals_builder = gtactb.ControlTotalsBuilder(config)
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
                                                          self._persons_households)

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

        # self._zones = self._zones[self._zones.PD > 0]

        # read in csv data
        self._persons_base = pd.read_csv(f"{self._config['PersonsSeedFile']}")
        self._households_base = pd.read_csv(f"{self._config['HouseholdsSeedFile']}",
                                            dtype={'HouseholdZone': int})

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
                                            how="left")

        self._filter_records()

        return

    def _filter_records(self):
        """
        Filters household and persons records from invalid zones and PDs
        :return:
        """
        self._households_base = self._households_base[self._households_base.PD > 0]
        self._persons_households = self._persons_households[self._persons_households.PD > 0]
        self._persons_households = self._persons_households[
            self._persons_households['HouseholdZone'].isin(InputProcessor._ZONE_RANGE)]

        self._households_base = self._households_base[
            self._households_base['HouseholdZone'].isin(InputProcessor._ZONE_RANGE)]
        self._persons_households.sort_values(by=['HouseholdZone', 'PD'],
                                             ascending=True).reset_index(inplace=True)
        return


    def _assign_puma_values(self):
        """
        Assigns the associated puma assignment from PD values from predefined setting
        :return:
        """
        self._households_base['puma'] = 0
        for index, pd_range in enumerate(InputProcessor._PUMA_PD_RANGES):
            self._households_base.loc[self._households_base['PD'].isin(pd_range), 'puma'] = index + 1

    def _preprocess_persons(self):
        """
        Process any person specific attributes before the control generation stage.
        """
        # clear certain employment zones for records
        self._persons_base.loc[self._persons_base.EmploymentZone < 6000,
                               'EmploymentZone'] = 0
        self._persons_base.rename(columns={'ExpansionFactor': 'weightp'}, inplace=True)

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
        self._persons_households = self._persons_households[
            (self._persons_households.EmploymentStatus != '9') &
            (self._persons_households.Occupation != '9')]

        self._postprocess_households()
        self._postprocess_persons()

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
        households.to_csv(f"{self._config['ProcessedHouseholdsSeedFile']}", index=False)

    def _postprocess_persons(self):
        """
        Performs any post process modifications to person level attributes, such as value mapping
        or filtering of invalid or unwanted records.
        :return:
        """
        persons = self._persons_households[
            ['HouseholdId', 'puma', 'PersonNumber', 'Age', 'Sex', 'License', 'EmploymentStatus',
             'Occupation', 'StudentStatus', 'EmploymentZone', 'weightp']].copy()
        persons.rename(columns={'weightp': 'weight'}, inplace=True)

        persons = persons[
            (persons.EmploymentStatus != '9') & (persons.Occupation != '9')]

        # map any persons attributes to the values specified in the configuration
        for mapping in self._config['CategoryMapping']['Persons'].items():
            persons[mapping[0]] = persons[mapping[0]].map(mapping[1])

        persons.sort_values(by=['HouseholdId'], ascending=True).reset_index(inplace=True)

        persons.to_csv(f"{self._config['ProcessedPersonsSeedFile']}", index=False)
