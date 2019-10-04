from shutil import copyfile

import pandas as pd
from gtamodel_popsyn.constants import *
from gtamodel_popsyn._gtamodel_popsyn_processor import GTAModelPopSynProcessor


class ControlTotalsBuilder(GTAModelPopSynProcessor):
    """
    ControlTotalsBuilder analyzes the input household and persons data and generates control totals
    for the specified attributes across all levels of geography.
    """

    @staticmethod
    def _sum_column(group, column, value, weight='weighth'):
        return group[group[column] == value][weight].sum()

    @staticmethod
    def _sum_column_gte(group, column, value, weight='weighth'):
        return group[group[column] >= value][weight].sum()

    @staticmethod
    def _sum_column_range(group, column, value, value2, weight='weighth'):
        return group[(group[column] >= value) & (group[column] <= value2)][weight].sum()

    def __init__(self, gtamodel_popsyn_instance, population_vector_file: str = None):
        """

        :param gtamodel_popsyn_instance:
        :param population_vector_file:
        """
        GTAModelPopSynProcessor.__init__(self, gtamodel_popsyn_instance)
        self._population_vector = None

        if population_vector_file is not None:
            self._population_vector = pd.read_csv(population_vector_file, index_col=0)
        self._zones = pd.DataFrame()
        self._age_bin_columns = []
        for age_bin in AGE_BINS:
            self._age_bin_columns.append(f'age{age_bin.start}_{age_bin.stop}')

        self._controls = pd.DataFrame(columns=['region',
                                               'puma', 'taz', 'maz', 'totalhh', 'totpop', 'S_O', 'S_S', 'S_P',
                                               'license_Y', 'license_N', 'E_O', 'E_F', 'E_P', 'E_J', 'E_H',
                                               'P', 'G', 'S', 'M', 'O' +
                                               'age65p', 'hhsize1', 'hhsize2', 'hhsize3', 'hhsize4p',
                                               'income_class_1', 'income_class_2', 'income_class_3',
                                               'income_class_4', 'income_class_5', 'income_class_6',
                                               'male', 'female',
                                               'employment_zone_internal',
                                               'employment_zone_external',
                                               'employment_zone_roaming'] + self._age_bin_columns)

    def _process_household_total(self, hh_group):
        """

        :param hh_group:
        :return:
        """
        self._controls['totalhh'] = hh_group.weighth.sum()
        return

    def _process_population_total(self, persons_group):
        """
        Processes the total population for the controls. Will use a population vector if it was supplied
        by the user, otherwise the population total is calculated from the input data.
        :param persons_group:
        :return:
        """
        if self._population_vector is None:
            self._controls['totpop'] = persons_group.weightp.sum()
        else:
            self._controls['totpop'] = self._population_vector['Population']

        return

    def build_control_totals(self, households, persons_households, zones):
        """
        Builds control totals for all levels of geography
        :param households:
        :param persons_households:
        :param zones:
        :return:
        """
        self._zones = zones[zones['Zone#'].isin(ZONE_RANGE)]

        hh2_group = households.groupby(['HouseholdZone'])
        hh_group = persons_households.groupby(['HouseholdZone'])

        self._controls['maz'] = self._zones['Zone#']
        self._controls['puma'] = 0
        self._controls['taz'] = self._zones['Zone#']
        self._controls = self._controls.set_index('maz')
        self._zones = self._zones.set_index('Zone#')
        self._controls['region'] = 1

        self._process_population_total(hh_group)
        self._process_household_total(hh2_group)

        self._controls['puma'] = (self._zones['puma'].astype(int))
        self._controls['male'] = hh_group.apply(lambda x: self._sum_column(x, 'Sex', 'M', 'weightp'))
        self._controls['female'] = \
            hh_group.apply(lambda x: self._sum_column(x, 'Sex', 'F', 'weightp'))

        self._controls['employment_zone_internal'] = \
            hh_group.apply(lambda x: x[x.EmploymentZone.isin(ZONE_RANGE)]['weightp'].sum())
        self._controls['employment_zone_roaming'] = \
            hh_group.apply(lambda x: x[x.EmploymentZone == ROAMING_ZONE_ID]['weightp'].sum())
        self._controls['employment_zone_external'] = \
            hh_group.apply(
                lambda x: x.loc[(x.EmploymentZone >= EXTERNAL_ZONE_RANGE.start) &
                                (x.EmploymentZone != ROAMING_ZONE_ID), 'weightp'].sum())
        self._controls['employment_zone_0'] = \
            hh_group.apply(
                lambda x: x.loc[(x.EmploymentZone == 0), 'weightp'].sum())

        self._controls['income_class_1'] = hh2_group.apply(
            lambda x: self._sum_column(x, 'IncomeClass', 1, 'weighth'))
        self._controls['income_class_2'] = hh2_group.apply(
            lambda x: self._sum_column(x, 'IncomeClass', 2, 'weighth'))
        self._controls['income_class_3'] = hh2_group.apply(
            lambda x: self._sum_column(x, 'IncomeClass', 3, 'weighth'))
        self._controls['income_class_4'] = hh2_group.apply(
            lambda x: self._sum_column(x, 'IncomeClass', 4, 'weighth'))
        self._controls['income_class_5'] = hh2_group.apply(
            lambda x: self._sum_column(x, 'IncomeClass', 5, 'weighth'))
        self._controls['income_class_6'] = hh2_group.apply(
            lambda x: self._sum_column(x, 'IncomeClass', 6, 'weighth'))

        self._controls['hhsize1'] = hh2_group.apply(
            lambda x: self._sum_column(x, 'NumberOfPersons', 1, 'weighth'))
        self._controls['hhsize2'] = hh2_group.apply(
            lambda x: self._sum_column(x, 'NumberOfPersons', 2, 'weighth'))
        self._controls['hhsize3'] = hh2_group.apply(
            lambda x: self._sum_column(x, 'NumberOfPersons', 3, 'weighth'))
        self._controls['hhsize4p'] = hh2_group.apply(
            lambda x: self._sum_column_gte(x, 'NumberOfPersons', 4, 'weighth'))

        for index, age_bin in enumerate(self._age_bin_columns):
            self._controls[age_bin] = hh_group.apply(
                lambda x: self._sum_column_range(x, 'Age', AGE_BINS[index].start, AGE_BINS[index].stop,
                                                 'weightp'))
        self._controls['E_J'] = hh_group.apply(
            lambda x: self._sum_column(x, 'EmploymentStatus', 'J', 'weightp'))
        self._controls['E_P'] = hh_group.apply(
            lambda x: self._sum_column(x, 'EmploymentStatus', 'P', 'weightp'))
        self._controls['E_F'] = hh_group.apply(
            lambda x: self._sum_column(x, 'EmploymentStatus', 'F', 'weightp'))
        self._controls['E_O'] = hh_group.apply(
            lambda x: self._sum_column(x, 'EmploymentStatus', 'O', 'weightp'))
        self._controls['E_H'] = hh_group.apply(
            lambda x: self._sum_column(x, 'EmploymentStatus', 'H', 'weightp'))

        self._controls['P'] = hh_group.apply(lambda x: self._sum_column(x, 'Occupation', 'P', 'weightp'))
        self._controls['G'] = hh_group.apply(lambda x: self._sum_column(x, 'Occupation', 'G', 'weightp'))
        self._controls['S'] = hh_group.apply(lambda x: self._sum_column(x, 'Occupation', 'S', 'weightp'))
        self._controls['M'] = hh_group.apply(lambda x: self._sum_column(x, 'Occupation', 'M', 'weightp'))
        self._controls['O'] = hh_group.apply(lambda x: self._sum_column(x, 'Occupation', 'O', 'weightp'))

        self._controls['license_Y'] = hh_group.apply(lambda x: self._sum_column(x, 'License', 'Y', 'weightp'))
        self._controls['license_N'] = hh_group.apply(lambda x: self._sum_column(x, 'License', 'N', 'weightp'))
        self._controls['S_O'] = hh_group.apply(lambda x: self._sum_column(x, 'StudentStatus', 'O', 'weightp'))
        self._controls['S_S'] = hh_group.apply(lambda x: self._sum_column(x, 'StudentStatus', 'S', 'weightp'))
        self._controls['S_P'] = hh_group.apply(lambda x: self._sum_column(x, 'StudentStatus', 'P', 'weightp'))

        self._controls = self._controls.fillna(0)
        self._write_maz_control_totals_file()
        taz_controls = self._write_taz_control_totals_file()
        self._write_meta_control_totals_file(taz_controls)

        return

    def _write_maz_control_totals_file(self):
        """

        :return:
        """
        maz_controls = self._controls.reset_index()[
            (['region', 'puma', 'taz', 'maz', 'totalhh', 'totpop', 'S_O', 'S_S', 'S_P',
              'license_Y', 'license_N', 'E_O', 'E_F', 'E_P', 'E_J', 'E_H', 'P',
              'G',
              'S', 'M', 'O'] + self._age_bin_columns +
             ['hhsize1', 'hhsize2', 'hhsize3', 'hhsize4p',
              'income_class_1',
              'income_class_2',
              'income_class_3',
              'income_class_4',
              'income_class_5',
              'income_class_6',
              'male',
              'female',
              'employment_zone_internal',
              'employment_zone_external',
              'employment_zone_roaming',
              'employment_zone_0'
              ])].sort_values(['puma', 'taz', 'maz']).drop(
            self._config['DropControlColumns'], axis=1)

        maz_controls[(maz_controls['totpop'] > 0) & (maz_controls['totalhh'] > 0)].astype(int).to_csv(
            f"{self._output_path}/Inputs/{self._config['MazLevelControls']}", index=False)

    def _write_taz_control_totals_file(self):
        """

        :return:
        """
        controls_taz = self._controls.reset_index()[(['region',
                                                      'puma', 'taz', 'totalhh', 'totpop', 'S_O', 'S_S', 'S_P',
                                                      'license_Y', 'license_N', 'E_O', 'E_F', 'E_P', 'E_J', 'E_H',
                                                      'P', 'G', 'S', 'M', 'O'] +
                                                     self._age_bin_columns +
                                                     ['hhsize1', 'hhsize2', 'hhsize3', 'hhsize4p',
                                                      'income_class_1', 'income_class_2', 'income_class_3',
                                                      'income_class_4', 'income_class_5', 'income_class_6',
                                                      'male', 'female',
                                                      'employment_zone_internal',
                                                      'employment_zone_external',
                                                      'employment_zone_roaming',
                                                      'employment_zone_0'])].sort_values(['puma', 'taz'])

        controls_taz[(controls_taz['totpop'] > 0) & (controls_taz['totalhh'] > 0)].drop(
            self._config['DropControlColumns'], axis=1).astype(int).to_csv(
            f"{self._output_path}/Inputs/{self._config['TazLevelControls']}", index=False)

        return controls_taz

    def _write_meta_control_totals_file(self, taz_controls):
        """
        
        :param taz_controls: The taz level controls, used as base to generate meta level controls.
        :return: 
        """
        meta_controls = taz_controls.groupby(['region'])[(['totalhh', 'totpop', 'S_O', 'S_S', 'S_P',
                                                           'P', 'G', 'S', 'M', 'O'] +
                                                          self._age_bin_columns + [
                                                              'E_O', 'E_F', 'E_P', 'E_J', 'E_H',
                                                              'income_class_1',
                                                              'income_class_2',
                                                              'income_class_3',
                                                              'income_class_4',
                                                              'income_class_5',
                                                              'income_class_6',
                                                              'male',
                                                              'female',
                                                              'employment_zone_internal',
                                                              'employment_zone_external',
                                                              'employment_zone_roaming',
                                                              'employment_zone_0'])].apply(sum).drop(
            self._config['DropControlColumns'], axis=1).reset_index()

        meta_controls.astype(int).to_csv(f"{self._output_path}/Inputs/{self._config['MetaLevelControls']}", index=False)
