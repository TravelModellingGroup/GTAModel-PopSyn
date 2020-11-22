from io import TextIOWrapper
from shutil import copyfile

import pandas as pd
from gtamodel_popsyn.constants import *
from gtamodel_popsyn._gtamodel_popsyn_processor import GTAModelPopSynProcessor
from gtamodel_popsyn.util.generate_zone_ranges import generate_zone_ranges


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

    def __init__(self, gtamodel_popsyn_instance):
        """

        :param gtamodel_popsyn_instance:
        """
        GTAModelPopSynProcessor.__init__(self, gtamodel_popsyn_instance)
        self._zones = pd.DataFrame()
        self._age_bin_columns = []
        self._population_vector = None
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

    def apply_population_vector(self, maz_file: str, taz_file: str, meta_file: str,
                                population_vector: TextIOWrapper):
        """
        Applies a population vector to an existing set of controls. Population and households
        and other control data existing in the config file are scaled by the population differences.

        Where base year data is missing, a value will be generated from the averages of nearby
        zones.
        @param population_vector:
        @param maz_file:
        @param taz_file:
        @param meta_file:
        @return:
        """
        maz: pd.DataFrame = pd.read_csv(maz_file)
        taz: pd.DataFrame = pd.read_csv(taz_file)
        meta: pd.DataFrame = pd.read_csv(meta_file)
        popvec: pd.DataFrame = pd.read_csv(population_vector)

        # map new puma values if input processing step was skipped
        maz, taz = self._map_control_puma_values(maz, taz)

        totpop = self.popsyn_config.total_population_column_name
        totalhh = self.popsyn_config.total_households_column_name

        maz['maz_index'] = maz['maz']
        taz['taz_index'] = taz['taz']
        maz.set_index('maz_index', inplace=True)
        taz.set_index('taz_index', inplace=True)
        popvec.set_index(popvec.columns[0], inplace=True)

        orig = maz.copy()

        # merge zones and population vector to only include zones that exist in source controls
        maz_merge = pd.merge(maz, popvec, left_index=True, right_index=True, how="outer").fillna(0)
        taz = pd.merge(taz, popvec, left_index=True, right_index=True, how="outer").fillna(0)
        maz = maz_merge

        maz.loc[maz['region'] == 0, 'region'] = 1
        maz.loc[maz['puma'] == 0, 'puma'] = 3

        taz.loc[taz['region'] == 0, 'region'] = 1
        taz.loc[taz['puma'] == 0, 'puma'] = 3

        maz_merge['maz'] = maz_merge.index
        maz_merge['taz'] = maz_merge.index
        taz['maz'] = taz.index
        taz['taz'] = taz.index

        controls_t1 = maz_merge[(maz_merge[totpop] > 0) & (maz_merge[maz_merge.columns[-1]] > 0)]
        controls_t2 = maz_merge[(maz_merge[totpop] == 0) & (maz_merge[maz_merge.columns[-1]] > 0)]
        controls_t3 = maz_merge[maz_merge[maz_merge.columns[-1]] == 0]

        # calculate population ratio where valid data exists between two populations
        ratio = controls_t1[controls_t1[controls_t1.columns[-1]] > 0][controls_t1.columns[-1]] / \
                controls_t1[controls_t1[totpop] > 0][totpop]

        # calculate mean values at PD level for possible missing base year data
        # (0 population total)
        maz_pd_group = orig.loc[controls_t1.index].groupby('puma')
        maz_pd_total = orig.loc[controls_t1.index].groupby('puma')[totpop].sum()

        for column in self.popsyn_config.person_control_columns:
            if column not in maz.columns:
                continue
            maz.loc[ratio.index, column] = controls_t1.loc[ratio.index, column] * ratio
            taz.loc[ratio.index, column] = controls_t1.loc[ratio.index, column] * ratio

            # apply mean aggregate values for specified columns where base year is 0
            column_ratio = (maz_pd_group[column].sum() / maz_pd_total)
            for planning_district in maz_pd_total.index:
                if planning_district == 0:
                    continue
                if column == totpop:
                    maz.loc[controls_t2.index, column] = controls_t2.loc[controls_t2.index, controls_t2.columns[-1]]
                    taz.loc[controls_t2.index, column] = controls_t2.loc[controls_t2.index, controls_t2.columns[-1]]
                    continue

                maz.loc[controls_t2.index, column] = column_ratio[planning_district] * maz.loc[
                    controls_t2.index, totpop]
                taz.loc[controls_t2.index, column] = column_ratio[planning_district] * maz.loc[
                    controls_t2.index, totpop]

            # zero out columns where new population is 0
            maz.loc[controls_t3.index, column] = 0
            taz.loc[controls_t3.index, column] = 0

        for column in self.popsyn_config.household_control_columns:
            if column not in maz.columns:
                continue
            maz.loc[ratio.index, column] = controls_t1.loc[ratio.index, column] * ratio
            taz.loc[ratio.index, column] = controls_t1.loc[ratio.index, column] * ratio

            # apply mean aggregate values where data is missing
            column_ratio = (maz_pd_group[column].sum() / maz_pd_total)
            for planning_district in maz_pd_total.index:
                if planning_district == 0:
                    continue
                maz.loc[controls_t2.index, column] = column_ratio[planning_district] * maz.loc[
                    controls_t2.index, totpop]
                taz.loc[controls_t2.index, column] = column_ratio[planning_district] * maz.loc[
                    controls_t2.index, totpop]

            # zero out columns where new population is 0
            maz.loc[controls_t3.index, column] = 0
            taz.loc[controls_t3.index, column] = 0

        # where no prior population data exists, calculate the household to population ratio

        meta[totpop] = maz[totpop].sum()
        meta[totalhh] = maz[totalhh].sum()

        # remove unused columns for control file clarity
        maz.drop(set(maz.columns) - set(
            ['region', 'puma', 'taz', 'maz'] +
            self.popsyn_config.person_control_columns +
            self.popsyn_config.household_control_columns), axis=1, inplace=True)
        # remove unused taz control columns
        taz.drop(set(taz.columns) - set(
            ['region', 'puma', 'taz'] +
            self.popsyn_config.person_control_columns +
            self.popsyn_config.household_control_columns), axis=1, inplace=True)

        maz = maz.astype(int)
        taz = taz.astype(int)
        meta = meta.astype(int)
        return maz, taz, meta

    def _map_control_puma_values(self, maz: pd.DataFrame, taz: pd.DataFrame):
        pd_ranges = []
        for r in self._config['PdGroups']:
            pd_ranges.append(range(r[0], r[1]))

        maz = maz.merge(self.popsyn_config.zone_pd_map, left_on="maz", right_index=True)
        taz = taz.merge(self.popsyn_config.zone_pd_map, left_on="taz", right_index=True)

        for index, pd_range in enumerate(pd_ranges):
            maz.loc[maz['PD'].between(pd_range.start, pd_range.stop, inclusive=True), ['puma']] = index + 1
            taz.loc[taz['PD'].between(pd_range.start, pd_range.stop, inclusive=True), ['puma']] = index + 1
        return (maz, taz)

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

    def _find_missing(self, lst):
        return [x for x in range(lst[0], lst[-1] + 1)
                if x not in lst]

    def build_control_totals(self, households, persons_households, zones):
        """
        Builds control totals for all levels of geography
        :param households:
        :param persons_households:
        :param zones:
        :return:
        """

        self._zones = zones[zones['Zone'].isin(self.popsyn_config.internal_zone_range)]

        hh2_group = households.groupby(['HouseholdZone'])
        hh_group = persons_households.groupby(['HouseholdZone'])

        # zones_empty = self._find_missing(self._zones['Zone#'].to_list())

        self._controls['maz'] = self._zones['Zone']
        self._controls['puma'] = 0
        self._controls['taz'] = self._zones['Zone']
        self._controls = self._controls.set_index('maz')
        self._zones = self._zones.set_index('Zone')
        self._controls['region'] = 1

        self._process_population_total(hh_group)
        self._process_household_total(hh2_group)

        self._controls['puma'] = (self._zones['puma'].astype(int))
        self._controls['male'] = hh_group.apply(lambda x: self._sum_column(x, 'Sex', 'M', 'weightp'))
        self._controls['female'] = \
            hh_group.apply(lambda x: self._sum_column(x, 'Sex', 'F', 'weightp'))

        self._controls['employment_zone_internal'] = \
            hh_group.apply(lambda x: x[x.EmploymentZone.isin(self.popsyn_config.internal_zone_range)]['weightp'].sum())
        self._controls['employment_zone_roaming'] = \
            hh_group.apply(lambda x: x[x.EmploymentZone == ROAMING_ZONE_ID]['weightp'].sum())
        self._controls['employment_zone_external'] = \
            hh_group.apply(
                lambda x: x.loc[(x.EmploymentZone.isin(self.popsyn_config.external_zone_range)) &
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
              ])].sort_values(['puma', 'taz', 'maz'])

        if self._config['DropControlColumns']:
            maz_controls = maz_controls.drop(self._config['DropControlColumns'], axis=1)

        maz_controls.astype(int).to_csv(
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

        if self._config['DropControlColumns']:
            controls_taz = controls_taz.drop(self._config['DropControlColumns'], axis=1)

        controls_taz.astype(int).to_csv(
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
