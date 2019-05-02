import sqlalchemy
from pandas import DataFrame
from sqlalchemy import create_engine
import pandas as pd

from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey, SmallInteger
from sqlalchemy import inspect

class DatabaseProcessor(object):
    """
    Processes input database tables for PopSyn3 - will create them where they do not exist,
    and read in the appropriate input data.
    """

    def __init__(self, config):
        self._config = config
        self._engine = None
        self._connection = None




    def initialize_database(self, persons=None, households=None):
        """
        Initializes all databases and tables for PopSyn3 execution
        :param persons: Processed persons dataframe.
        :param households: Processed households dataframe.
        :return:
        """
        self._engine = create_engine(
            f'mysql+pymysql://{self._config["DatabaseUser"]}:{self._config["DatabasePassword"]}'
            f'@{self._config["DatabaseServer"]}')
        self._connection = self._engine.connect()

        self._connection.execute(f'CREATE DATABASE IF NOT EXISTS {self._config["DatabaseName"]}')

    def _initialize_record_tables(self, persons: DataFrame = None, households: pd.DataFrame = None):

        if persons is None:
            persons: pd.DataFrame = pd.read_csv(f"{self._config['ProcessedPersonsSeedFile']}")

        if households is None:
            households: pd.DataFrame = pd.read_csv(f"{self._config['ProcessedHouseholdsSeedFile']}")


        # [persons.dtypes[c] for c in persons.columns]

        households_table = Table()

        persons.to_sql('pumf_person', self._connection, if_exists='replace', index=False)
        households.to_sql('pumf_hh', self._connection, if_exists='replace', index=False)

        self._connection.execute('CREATE TABLE hhtable LIKE pumf_hh;')
        self._connection.execute('CREATE TABLE perstable LIKE pumf_person;')
        self._connection.execute('INSERT INTO hhtable (SELECT * FROM pumf_hh);')
        self._connection.execute('INSERT INTO perstable (SELECT * FROM pumf_person);')
        self._connection.execute('ALTER TABLE hhtable ADD COLUMN hhnum INT NOT NULL AUTO_INCREMENT UNIQUE;')
        self._connection.execute('ALTER TABLE perstable	ADD COLUMN hhnum INT NULL;')
        self._connection.execute('UPDATE perstable a LEFT JOIN hhtable b '
                                 'ON a.HouseholdId=b.HouseholdId SET a.hhnum=b.hhnum;')
        return

    def _initialize_control_tables(self, maz_controls: pd.DataFrame = None,
                                   taz_controls: pd.DataFrame = None,
                                   meta_controls: pd.DataFrame = None):
        """
        Initializes database control tables for each level of geography
        :param maz_controls:
        :param taz_controls:
        :param meta_controls:
        :return:
        """

        if meta_controls is None:
            meta_controls = pd.read_csv(f"{self._config['MetaLevelControls']}")

        if maz_controls is None:
            maz_controls = pd.read_csv(f"{self._config['MazLevelControls']}")

        if taz_controls is None:
            taz_controls = pd.read_csv(f"{self._config['TazLevelControls']}")

        maz_controls.to_sql('control_totals_maz', self._connection, if_exists='replace', index=False)
        taz_controls.to_sql('control_totals_taz', self._connection, if_exists='replace', index=False)
        meta_controls.to_sql('control_totals_meta', self._connection, if_exists='replace', index=False)
        return
