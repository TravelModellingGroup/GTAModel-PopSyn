import pandas
from logzero import setup_logger
from sqlalchemy import create_engine
import gtamodel_popsyn.sql_commands as sql_commands
from gtamodel_popsyn.constants import *
from gtamodel_popsyn._gtamodel_popsyn_processor import GTAModelPopSynProcessor
import os


class OutputProcessor(GTAModelPopSynProcessor):
    """
    Performs processing on PopSyn3's output data.
    Synthesized persons and households are extracted from the processing database and
    transformed into the input format expected by the GTAModel runtime.
    """

    def __init__(self, gtamodel_popsyn_instance, percent_population: float):
        """

        :param gtamodel_popsyn_instance:
        :param percent_population:
        """
        GTAModelPopSynProcessor.__init__(self, gtamodel_popsyn_instance)
        self._db_connection = None
        self._persons = pandas.DataFrame()
        self._households = pandas.DataFrame()
        self._persons_households = pandas.DataFrame()
        self._output_folder = self._output_path
        self._percent_population = percent_population
        self._engine = create_engine(
            f'mysql+pymysql://{self._config["DatabaseUser"]}:'
            f'{self._config["DatabasePassword"]}@{self._config["DatabaseServer"]}/{self._config["DatabaseName"]}'
        )

        try:
            os.makedirs(f'{self._output_folder}/ZonalResidence/')
            os.makedirs(f'{self._output_folder}/HouseholdData/')
        except FileExistsError:
            pass

        return

    def _read_persons_households(self):
        """
        Read persons and households from database and scale expansion factor by % population.
        :return:
        """
        self._logger.info("Reading persons and households records from database.")
        self._persons = pandas.read_sql_table('gta_persons', self._db_connection)

        self._persons['ExpansionFactor'] = self._persons['ExpansionFactor'] * (1.0 / self._percent_population)

        self._households = pandas.read_sql_table('gta_households', self._db_connection)

        self._households['ExpansionFactor'] = self._households['ExpansionFactor'] * (1.0 / self._percent_population)
        return

    def _process_persons(self):
        """

        :return:
        """
        self._logger.info("Remapping category attributes.")
        for mapping in self._config['CategoryMapping']['Persons'].items():
            inverted_map = {value: key for key, value in mapping[1].items()}
            self._persons.loc[:, mapping[0]] = self._persons.loc[:, mapping[0]].map(inverted_map)

        # self._persons.loc[(self._persons['EmploymentZone'] < ZONE_RANGE.stop)] = 0

        return

    def _process_households(self):

        return

    def _process_persons_households(self):

        self._persons_households = pandas.merge(left=self._persons, right=self._households, left_on="HouseholdId",
                                                right_on="HouseholdId")
        self._persons_households.rename(columns={'ExpansionFactor_x': 'Persons'}, inplace=True)

    def _gta_model_transform(self):
        """
        Creates two new database tables representing the final synthesizes households and persons
        with all extra columns either removed or renamed to match the expected input column names
        of GTAModel.
        :return:
        """
        self._logger.info("Transforming synthetic population database tables.")
        for command in sql_commands.TRANSFORM_PERSONS_TABLE_SQL_COMMANDS:
            self._db_connection.execute(command)

        for command in sql_commands.TRANSFORM_HOUSEHOLDS_TABLE_SQL_COMMANDS:
            self._db_connection.execute(command)

        return

    def _write_persons_file(self):
        """
        Outputs file HouseholdData/Persons.csv
        If any attribute mappings are present, they will be applied to the output data.
        :return:
         """
        self._logger.info("Writing synthesized population (persons) to file.")

        # set internal employment zones to 0
        # self._persons[self._persons['EmploymentZone'] < INTERNAL_ZONE_RANGE.stop] = 0
        self._persons.to_csv(f'{self._output_folder}/HouseholdData/Persons.csv', index=False)
        return

    def _write_households_file(self):
        """
        Outputs file HouseholdData/Households.csv
        :return:
        """
        self._logger.info("Writing synthesized population (households) to file.")
        self._households.to_csv(f'{self._output_folder}/HouseholdData/Households.csv', index=False)
        return

    def _process_household_totals(self):
        """

        :return:
        """
        self._logger.info("Writing synthesized population (household totals) to file.")
        self._households[['HouseholdZone', 'ExpansionFactor']] \
            .groupby('HouseholdZone').agg({'ExpansionFactor': sum}).reset_index().rename(
            columns={'HouseholdZone': 'Zone', 'ExpansionFactor': 'ExpandedHouseholds'}).to_csv(
            f'{self._output_folder}/HouseholdData/HouseholdTotals.csv', index=False)
        return

    def _process_zonal_residences(self):
        """
        Creates zonal residence files for occupation and employment statuses.
        :return:
        """

        self._logger.info("Processing zonal residence information for occupation and employment.")

        internal_persons_households = self._persons_households.loc[self._persons_households.EmploymentZone < INTERNAL_ZONE_RANGE.stop].copy()
        internal_persons_households.rename(columns={'HouseholdZone': 'Zone'}, inplace=True)

        gta_ph_grouped = internal_persons_households.groupby(['Zone', 'Occupation', 'EmploymentStatus'], as_index=False,
                                                             group_keys=False)[
            'Persons'].apply(sum)

        # gta_ph_grouped.reset_index().to_csv('temp/all.csv',index=False)

        gta_ph_grouped[:, 'G', 'F'].reset_index().to_csv(f'{self._output_folder}/ZonalResidence/GF.csv',
                                                         index=False)
        gta_ph_grouped[:, 'G', 'P'].reset_index().to_csv(f'{self._output_folder}/ZonalResidence/GP.csv',
                                                         index=False)
        gta_ph_grouped[:, 'M', 'F'].reset_index().to_csv(f'{self._output_folder}/ZonalResidence/MF.csv',
                                                         index=False)
        gta_ph_grouped[:, 'M', 'P'].reset_index().to_csv(f'{self._output_folder}/ZonalResidence/MP.csv',
                                                         index=False)
        gta_ph_grouped[:, 'P', 'F'].reset_index().to_csv(f'{self._output_folder}/ZonalResidence/PF.csv',
                                                         index=False)
        gta_ph_grouped[:, 'P', 'P'].reset_index().to_csv(f'{self._output_folder}/ZonalResidence/PP.csv',
                                                         index=False)
        gta_ph_grouped[:, 'S', 'F'].reset_index().to_csv(f'{self._output_folder}/ZonalResidence/SF.csv',
                                                         index=False)
        gta_ph_grouped[:, 'S', 'P'].reset_index().to_csv(f'{self._output_folder}/ZonalResidence/SP.csv',
                                                         index=False)
        return

    def _read_persons_households_file(self):
        """
        Reads persons and households from saved records file, rather than the configured database.
        Percent population is not taken into account as it will already exist in the output file.
        :return:
        """
        self._logger.info("Reading persons and households records from saved files.")

        self._persons = pandas.read_csv(f'{self._output_folder}/HouseholdData/Persons.csv')

        # self._persons['ExpansionFactor'] = self._persons['ExpansionFactor'] * (1.0 / self._percent_population)

        self._households = pandas.read_csv(f'{self._output_folder}/HouseholdData/Households.csv')

        # self._households['ExpansionFactor'] = self._households['ExpansionFactor'] * (1.0 / self._percent_population)

    def merge_outputs(self, merge_outputs: list):
        """

        return
        """
        self._logger.info(f'Merging outputs: {merge_outputs}')

        households_merge = pandas.read_csv(f'{self._output_folder}/{merge_outputs[0]}')
        self._households = self._households.append(households_merge, sort=False).fillna(0)[
            households_merge.columns.tolist()]

        persons_merge = pandas.read_csv(f'{self._output_folder}/{merge_outputs[1]}')
        self._persons = self._persons.append(persons_merge, sort=False).fillna(0)[self._persons.columns.tolist()]

        self._households.to_csv('test.csv', index=False)

        return

    def generate_outputs(self, use_saved: bool, merge_outputs: list):
        """
        Generates and writes all output files to the specified output location
        :return:
        """
        # Read and process household and persons data

        if not use_saved:
            self._db_connection = self._engine.connect()

            # Create and transform required db tables
            self._gta_model_transform()
            self._read_persons_households()
        else:
            self._read_persons_households_file()

        if len(merge_outputs) == 2:
            self.merge_outputs(merge_outputs)

        self._process_persons()
        self._process_households()

        self._process_persons_households()

        self._process_zonal_residences()

        # Process and write outputs
        self._write_households_file()
        self._write_persons_file()

        self._db_connection.close()

        self._logger.info("Finished output processing.")
        return

    def __del__(self):
        return
