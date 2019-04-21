import pandas as pd
import json


class ControlTotalsBuilder(object):
    """
    ControlTotalsBuilder analyzes the input household and persons data and generates control totals
    for the specified attributes across all levels of geography.
    """

    def _sum_column(self, group, column, value, weight='weighth'):
        return group[group[column] == value][weight].sum()

    def _sum_column_gte(self, group, column, value, weight='weighth'):
        return group[group[column] >= value][weight].sum()

    def _sum_column_range(self, group, column, value, value2, weight='weighth'):
        return group[(group[column] >= value) & (group[column] <= value2)][weight].sum()

    def __init__(self, config):
        """
        Initializes the control totals builder.
        :param config_file:
        """
        self._config = config
        self._controls = pd.DataFrame(columns=['region',
                                               'puma', 'taz', 'maz', 'totalhh', 'totpop', 'S_O', 'S_S', 'S_P',
                                               'license_Y'
            , 'license_N', 'E_O', 'E_F', 'E_P', 'E_J', 'E_H', 'P', 'G', 'S', 'M', 'O', 'age0_14', 'age15_29',
                                               'age30_44',
                                               'age45_64'
            , 'age65p', 'hhsize1', 'hhsize2', 'hhsize3', 'hhsize4p', 'numv1', 'numv2', 'numv3p',
                                               'income_class_1',
                                               'income_class_2',
                                               'income_class_3',
                                               'income_class_4',
                                               'income_class_5',
                                               'income_class_6',
                                               'male',
                                               'female'])

    def build_control_totals(self, households, persons_households):
        """
        Builds the control tables (represented as dataframes).
        :return:
        """
        hh2_group = households.groupby(['HouseholdZone'])
        hh_group = persons_households.groupby(['HouseholdZone'])

        self._controls['totalhh'] = hh2_group.weighth.sum().astype(int).to_list()
        self._controls['totpop'] = hh_group.weightp.sum().astype(int).to_list()

        self._controls['puma'] = households.groupby('HouseholdZone',as_index=False)['puma'].apply(lambda x: list(x)[0])
        self._controls['taz'] = households.groupby('HouseholdZone',as_index=False)['PD'].apply(lambda x: list(x)[0])


        self._controls['maz'] = hh_group.groups.keys()
        self._controls['male'] = hh_group.apply(lambda x: self._sum_column(x, 'Sex', 'M', 'weightp')).astype(
            int).to_list()
        self._controls['female'] = hh_group.apply(lambda x: self._sum_column(x, 'Sex', 'F', 'weightp')).astype(
            int).to_list()


        # households.groupby(['HouseholdZone']).apply(lambda x: x.groupby('NumberOfPersons')['ExpansionFactor'].apply(sum)).unstack()

        # ph.groupby(['HouseholdZone']).apply(lambda x: x.drop_duplicates('HouseholdId').groupby(['NumberOfPersons'])['ExpansionFactor_y'].apply(sum)).unstack()

        # ph.groupby(['HouseholdZone']).apply(lambda x: x.groupby(['Sex'])['ExpansionFactor_x'].apply(sum)).unstack()

        self._controls['income_class_1'] = hh2_group.apply(
            lambda x: self._sum_column(x, 'IncomeClass', 1, 'weighth')).astype(
            int).to_list()
        self._controls['income_class_2'] = hh2_group.apply(
            lambda x: self._sum_column(x, 'IncomeClass', 2, 'weighth')).astype(
            int).to_list()
        self._controls['income_class_3'] = hh2_group.apply(
            lambda x: self._sum_column(x, 'IncomeClass', 3, 'weighth')).astype(
            int).to_list()
        self._controls['income_class_4'] = hh2_group.apply(
            lambda x: self._sum_column(x, 'IncomeClass', 4, 'weighth')).astype(
            int).to_list()
        self._controls['income_class_5'] = hh2_group.apply(
            lambda x: self._sum_column(x, 'IncomeClass', 5, 'weighth')).astype(
            int).to_list()
        self._controls['income_class_6'] = hh2_group.apply(
            lambda x: self._sum_column(x, 'IncomeClass', 6, 'weighth')).astype(
            int).to_list()

        self._controls['numv1'] = hh2_group.apply(lambda x: self._sum_column(x, 'Vehicles', 1, 'weighth')).astype(
            int).to_list()
        self._controls['numv2'] = hh2_group.apply(lambda x: self._sum_column(x, 'Vehicles', 2, 'weighth')).astype(
            int).to_list()
        self._controls['numv3p'] = hh2_group.apply(lambda x: self._sum_column_gte(x, 'Vehicles', 3, 'weighth')).astype(
            int).to_list()

        self._controls['hhsize1'] = hh2_group.apply(
            lambda x: self._sum_column(x, 'NumberOfPersons', 1, 'weighth')).astype(
            int).to_list()
        self._controls['hhsize2'] = hh2_group.apply(
            lambda x: self._sum_column(x, 'NumberOfPersons', 2, 'weighth')).astype(
            int).to_list()
        self._controls['hhsize3'] = hh2_group.apply(
            lambda x: self._sum_column(x, 'NumberOfPersons', 3, 'weighth')).astype(
            int).to_list()
        self._controls['hhsize4p'] = hh2_group.apply(
            lambda x: self._sum_column_gte(x, 'NumberOfPersons', 4, 'weighth')).astype(
            int).to_list()

        self._controls['age0_14'] = hh_group.apply(lambda x: self._sum_column_range(x, 'Age', 0, 14, 'weightp')).astype(
            int).to_list()
        self._controls['age15_29'] = hh_group.apply(
            lambda x: self._sum_column_range(x, 'Age', 15, 29, 'weightp')).astype(
            int).to_list()
        self._controls['age30_44'] = hh_group.apply(
            lambda x: self._sum_column_range(x, 'Age', 30, 44, 'weightp')).astype(
            int).to_list()
        self._controls['age45_64'] = hh_group.apply(
            lambda x: self._sum_column_range(x, 'Age', 45, 64, 'weightp')).astype(
            int).to_list()
        self._controls['age65p'] = hh_group.apply(
            lambda x: self._sum_column_range(x, 'Age', 65, 2000, 'weightp')).astype(
            int).to_list()

        self._controls['E_J'] = hh_group.apply(
            lambda x: self._sum_column(x, 'EmploymentStatus', 'J', 'weightp')).astype(
            int).to_list()
        self._controls['E_P'] = hh_group.apply(
            lambda x: self._sum_column(x, 'EmploymentStatus', 'P', 'weightp')).astype(
            int).to_list()
        self._controls['E_F'] = hh_group.apply(
            lambda x: self._sum_column(x, 'EmploymentStatus', 'F', 'weightp')).astype(
            int).to_list()
        self._controls['E_O'] = hh_group.apply(
            lambda x: self._sum_column(x, 'EmploymentStatus', 'O', 'weightp')).astype(
            int).to_list()
        self._controls['E_H'] = hh_group.apply(
            lambda x: self._sum_column(x, 'EmploymentStatus', 'H', 'weightp')).astype(
            int).to_list()

        self._controls['P'] = hh_group.apply(lambda x: self._sum_column(x, 'Occupation', 'P', 'weightp')).astype(
            int).to_list()
        self._controls['G'] = hh_group.apply(lambda x: self._sum_column(x, 'Occupation', 'G', 'weightp')).astype(
            int).to_list()
        self._controls['S'] = hh_group.apply(lambda x: self._sum_column(x, 'Occupation', 'S', 'weightp')).astype(
            int).to_list()
        self._controls['M'] = hh_group.apply(lambda x: self._sum_column(x, 'Occupation', 'M', 'weightp')).astype(
            int).to_list()
        self._controls['O'] = hh_group.apply(lambda x: self._sum_column(x, 'Occupation', 'O', 'weightp')).astype(
            int).to_list()

        self._controls['license_Y'] = hh_group.apply(lambda x: self._sum_column(x, 'License', 'Y', 'weightp')).astype(
            int).to_list()
        self._controls['license_N'] = hh_group.apply(lambda x: self._sum_column(x, 'License', 'N', 'weightp')).astype(
            int).to_list()

        self._controls['S_O'] = hh_group.apply(lambda x: self._sum_column(x, 'StudentStatus', 'O', 'weightp')).astype(
            int).to_list()
        self._controls['S_S'] = hh_group.apply(lambda x: self._sum_column(x, 'StudentStatus', 'S', 'weightp')).astype(
            int).to_list()
        self._controls['S_P'] = hh_group.apply(lambda x: self._sum_column(x, 'StudentStatus', 'P', 'weightp')).astype(
            int).to_list()

        self._controls['region'] = 1

        self._write_maz_control_totals_file()
        self._write_taz_control_totals_file()
        self._write_meta_control_totals_file()

        return

    def _write_maz_control_totals_file(self):
        """

        :return:
        """
        self._controls[['region',
                        'puma', 'taz', 'maz', 'totalhh', 'totpop', 'S_O', 'S_S', 'S_P', 'license_Y'
            , 'license_N', 'E_O', 'E_F', 'E_P', 'E_J', 'E_H', 'P', 'G', 'S', 'M', 'O', 'age0_14', 'age15_29',
                        'age30_44',
                        'age45_64'
            , 'age65p', 'hhsize1', 'hhsize2', 'hhsize3', 'hhsize4p', 'numv1', 'numv2', 'numv3p',
                        'income_class_1',
                        'income_class_2',
                        'income_class_3',
                        'income_class_4',
                        'income_class_5',
                        'income_class_6',
                        'male',
                        'female']].sort_values(['puma','taz','maz']).\
            to_csv(f"{self._config['MazLevelControls']}", index=False)

    def _write_taz_control_totals_file(self):
        """

        :return:
        """
        controls_taz = self._controls.groupby('taz')[['totalhh', 'totpop', 'S_O', 'S_S', 'S_P', 'license_Y'
            , 'license_N', 'E_O', 'E_F', 'E_P', 'E_J', 'E_H', 'P', 'G', 'S', 'M', 'O', 'age0_14', 'age15_29',
                                                      'age30_44',
                                                      'age45_64'
            , 'age65p', 'hhsize1', 'hhsize2', 'hhsize3', 'hhsize4p', 'numv1', 'numv2', 'numv3p',
                                                      'income_class_1',
                                                      'income_class_2',
                                                      'income_class_3',
                                                      'income_class_4',
                                                      'income_class_5',
                                                      'income_class_6',
                                                      'male',
                                                      'female']].sum().reset_index()
        controls_taz['region'] = 1
        controls_taz['puma'] = self._controls.groupby('taz',as_index=False)['puma'].apply(lambda x: list(x)[0])

        controls_taz[['region',
                 'puma', 'taz', 'totalhh', 'totpop', 'S_O', 'S_S', 'S_P', 'license_Y'
            , 'license_N', 'E_O', 'E_F', 'E_P', 'E_J', 'E_H', 'P', 'G', 'S', 'M', 'O', 'age0_14', 'age15_29',
                 'age30_44',
                 'age45_64'
            , 'age65p', 'hhsize1', 'hhsize2', 'hhsize3', 'hhsize4p', 'numv1', 'numv2', 'numv3p',
                 'income_class_1',
                 'income_class_2',
                 'income_class_3',
                 'income_class_4',
                 'income_class_5',
                 'income_class_6',
                 'male',
                 'female']].sort_values(['puma','taz'])\
            .to_csv(f"{self._config['TazLevelControls']}", index=False)

    def _write_meta_control_totals_file(self):
        """
        Writes the meta control totals file.
        :param self:
        :return:
        """
        controls_meta = self._controls.groupby('region')[['totalhh', 'totpop',
                                         'P', 'G', 'S', 'M', 'O',
                                         'E_O', 'E_F', 'E_P', 'E_J', 'E_H',
                                         'income_class_1',
                                         'income_class_2',
                                         'income_class_3',
                                         'income_class_4',
                                         'income_class_5',
                                         'income_class_6']].sum().reset_index()
        controls_meta.to_csv(f"{self._config['MetaLevelControls']}", index=False)

