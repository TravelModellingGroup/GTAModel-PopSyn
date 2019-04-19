import pandas
from sqlalchemy import create_engine
import gtamodel_popsyn.sql_commands as sql_commands


class OutputProcessor(object):
    """
    Performs processing on PopSyn3's output data.
    Synthesized persons and households are extracted from the processing database and
    transformed into the input format expected by the GTAModel runtime.
    """

    def __init__(self, config):
        self._config = config
        self._db_connection = None
        self._engine = create_engine(
            f'mysql+pymysql://{config["DatabaseUser"]}:'
            f'{config["DatabasePassword"]}@{config["DatabaseServer"]}/{config["DatabaseName"]}'
        )

        return

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
        persons = pandas.read_sql_table('gta_households', self._db_connection)

        for mapping in self._config['CategoryMapping']['Persons'].items():
            inverted_map = {value: key for key, value in mapping[1].items()}
            persons[mapping[0]] = persons[mapping[0]].map(inverted_map)

        persons.to_csv(f'{self._config["OutputFolder"]}/HouseholdData/Persons.csv', index=False)
        return

    def _write_households_file(self):
        """
        Outputs file HouseholdData/Households.csv
        :return:
        """
        households = pandas.read_sql_table('gta_households', self._db_connection)
        households.to_csv(f'{self._config["OutputFolder"]}/HouseholdData/Households.csv', index=False)
        return

    def _write_household_totals_file(self):
        """
        Outputs file HouseholdData/HouseholdTotals.csv
        :return:
        """
        household_totals = pandas.read_sql_table('gta_household_totals', self._db_connection)
        household_totals.to_csv(f'{self._config["OutputFolder"]}/HouseholdData/HouseholdTotals.csv', index=False)
        return

    def generate_outputs(self):
        """
        Generates and writes all output files to the specified output location
        :return:
        """

        self._db_connection = self._engine.connect()

        # Create and transform required db tables
        self._gta_model_transform()

        # Process and write outputs
        self._write_household_totals_file()
        self._write_households_file()
        self._write_persons_file()

        self._db_connection.close()
        return

    def __del__(self):
        if self._db_connection.open:
            self._db_connection.close()
