import pandas
from sqlalchemy import create_engine
import gtamodel_popsyn.sql_commands as sql_commands
from gtamodel_popsyn.constants import *


class OutputProcessor(object):
    """
    Performs processing on PopSyn3's output data.
    Synthesized persons and households are extracted from the processing database and
    transformed into the input format expected by the GTAModel runtime.
    """

    def __init__(self, config):
        self._config = config
        self._db_connection = None
        self._persons = pandas.DataFrame()
        self._households = pandas.DataFrame()
        self._persons_households = pandas.DataFrame()
        self._engine = create_engine(
            f'mysql+pymysql://{config["DatabaseUser"]}:'
            f'{config["DatabasePassword"]}@{config["DatabaseServer"]}/{config["DatabaseName"]}'
        )

        return

    def _read_persons_households(self):
        self._persons = pandas.read_sql_table('gta_persons', self._db_connection)
        self._households = pandas.read_sql_table('gta_households', self._db_connection)
        return

    def _process_persons(self):

        for mapping in self._config['CategoryMapping']['Persons'].items():
            inverted_map = {value: key for key, value in mapping[1].items()}
            self._persons.loc[:, mapping[0]] = self._persons.loc[:, mapping[0]].map(inverted_map)

        self._persons[(self._persons['EmploymentZone'] < ZONE_RANGE.start) &
                      (self._persons['EmploymentZone'] != ROAMING_ZONE_ID)]['EmploymentZone'] = 0

        return

    def _process_households(self):

        return

    def _process_persons_households(self):
        self._persons_households = pandas.merge(left=self._persons, right=self._households, left_on="HouseholdId",right_on="HouseholdId")

    def _gta_model_transform(self):
        """
        Creates two new database tables representing the final synthesizes households and persons
        with all extra columns either removed or renamed to match the expected input column names
        of GTAModel.
        :return:
        """
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
        self._persons.to_csv(f'{self._config["OutputFolder"]}/HouseholdData/Persons.csv', index=False)
        return

    def _write_households_file(self):
        """
        Outputs file HouseholdData/Households.csv
        :return:
        """
        self._households.to_csv(f'{self._config["OutputFolder"]}/HouseholdData/Households.csv', index=False)
        return

    def _process_household_totals(self):
        """

        :return:
        """
        self._households[['HouseholdZone', 'ExpansionFactor']] \
            .groupby('HouseholdZone').agg({'ExpansionFactor': sum}).reset_index().rename(
            columns={'HouseholdZone': 'Zone', 'ExpansionFactor': 'ExpandedHouseholds'}).to_csv(
            f'{self._config["OutputFolder"]}/HouseholdData/HouseholdTotals.csv', index=False)
        return

    def _process_zonal_residences(self):
        """
        Creates zonal residence files for occupation and employment statuses.
        :return:
        """

        # print(self._persons_households)
        gta_ph_grouped = self._persons_households.groupby(['HouseholdZone', 'Occupation', 'EmploymentStatus'])['ExpansionFactor_x'].apply(sum)

        gta_ph_grouped[:, 'G', 'F'].reset_index().to_csv(f'{self._config["OutputFolder"]}/ZonalResidence/GF.csv',
                                                         index=False)
        gta_ph_grouped[:, 'G', 'P'].reset_index().to_csv(f'{self._config["OutputFolder"]}/ZonalResidence/GP.csv',
                                                         index=False)
        gta_ph_grouped[:, 'M', 'F'].reset_index().to_csv(f'{self._config["OutputFolder"]}/ZonalResidence/MF.csv',
                                                         index=False)
        gta_ph_grouped[:, 'M', 'P'].reset_index().to_csv(f'{self._config["OutputFolder"]}/ZonalResidence/MP.csv',
                                                         index=False)
        gta_ph_grouped[:, 'P', 'F'].reset_index().to_csv(f'{self._config["OutputFolder"]}/ZonalResidence/PF.csv',
                                                         index=False)
        gta_ph_grouped[:, 'P', 'P'].reset_index().to_csv(f'{self._config["OutputFolder"]}/ZonalResidence/PP.csv',
                                                         index=False)
        gta_ph_grouped[:, 'S', 'F'].reset_index().to_csv(f'{self._config["OutputFolder"]}/ZonalResidence/SF.csv',
                                                         index=False)
        gta_ph_grouped[:, 'S', 'P'].reset_index().to_csv(f'{self._config["OutputFolder"]}/ZonalResidence/SP.csv',
                                                         index=False)
        return

    def generate_outputs(self):
        """
        Generates and writes all output files to the specified output location
        :return:
        """

        self._db_connection = self._engine.connect()

        # Create and transform required db tables
        self._gta_model_transform()

        # Read and process household and persons data
        self._read_persons_households()

        self._process_persons()
        self._process_households()

        self._process_persons_households()

        self._process_zonal_residences()

        # Process and write outputs
        self._write_households_file()
        self._write_persons_file()
        self._db_connection.close()
        return

    def __del__(self):
        return
