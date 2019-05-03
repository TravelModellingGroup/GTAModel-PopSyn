import sqlalchemy
from pandas import DataFrame
from sqlalchemy import create_engine
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey, SmallInteger, Numeric, NVARCHAR, DECIMAL, \
    FLOAT, VARCHAR
from sqlalchemy import inspect
from sqlalchemy.engine import Connection, Engine
from gtamodel_popsyn._gtamodel_popsyn_processor import GTAModelPopSynProcessor

_PANDAS_DTYPE_SQL_TYPE = {
    'int64': Integer,
    'float64': FLOAT,
    'float32': FLOAT,
    'object': VARCHAR(1)
}


class DatabaseProcessor(GTAModelPopSynProcessor):
    """
    Processes input database tables for PopSyn3 - will create them where they do not exist,
    and read in the appropriate input data.
    """

    def __init__(self, gtamodel_popsyn_instance):
        """

        :param gtamodel_popsyn_instance:
        """
        GTAModelPopSynProcessor.__init__(self, gtamodel_popsyn_instance)
        self._engine: Engine = None
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
            f'@{self._config["DatabaseServer"]}/{self._config["DatabaseName"]}')
        self._connection = self._engine.connect()

        self._initialize_record_tables(persons, households)

        self._initialize_control_tables()
        self._connection.close()

    def _initialize_record_tables(self, persons: DataFrame = None, households: pd.DataFrame = None):
        """
        Initializes the database with input seed records. This data can be fed from the initial input generation
        step, otherwise the input data is assumed to be available on file from the previous input processing step.
        :param persons:
        :param households:
        :return:
        """

        if persons is None:
            persons: pd.DataFrame = pd.read_csv(f"{self._output_path}/Inputs/{self._config['ProcessedPersonsSeedFile']}")

        if households is None:
            households: pd.DataFrame = pd.read_csv(f"{self._output_path}/Inputs/{self._config['ProcessedHouseholdsSeedFile']}")

        metadata = MetaData()

        households_table_columns = [Column(c, _PANDAS_DTYPE_SQL_TYPE[households.dtypes[c].name]) for c in
                                    households.columns]
        households_table = Table('pumf_hh', metadata, *households_table_columns)
        for c in households.columns:
            if households.dtypes[c].name == 'float64':
                households[c] = households[c].astype('float32')
        households_table.columns[households.columns[0]].primary_key = True

        persons_table_columns = [Column(c, _PANDAS_DTYPE_SQL_TYPE[persons.dtypes[c].name]) for c in persons.columns]
        persons_table = Table('pumf_person', metadata, *persons_table_columns)
        for c in persons.columns:
            if persons.dtypes[c].name == 'float64':
                persons[c] = persons[c].astype('float32')

        persons_table.columns[persons.columns[0]].primary_key = True
        persons_table.columns[persons.columns[0]].primary_key = True

        metadata.drop_all(self._engine, [households_table, persons_table])
        metadata.create_all(self._engine)

        households.to_sql('pumf_hh', self._connection, if_exists='append', index=False)
        persons.to_sql('pumf_person', self._connection, if_exists='append', index=False)

        # new metadata
        metadata = MetaData()
        hhtable = Table('hhtable', metadata, *[Column(c, _PANDAS_DTYPE_SQL_TYPE[households.dtypes[c].name]) for c in
                                               households.columns],
                        Column('hhnum', Integer, unique=True, autoincrement=True, nullable=False))

        perstable = Table('perstable', metadata, *[Column(c, _PANDAS_DTYPE_SQL_TYPE[persons.dtypes[c].name]) for c in
                                                   persons.columns],
                          Column('hhnum', Integer))

        metadata.drop_all(self._engine, [hhtable, perstable])
        metadata.create_all(self._engine)
        hhtable_data = pd.concat([households, pd.Series(range(1, households.shape[0] + 1), dtype=int, name="hhnum")],
                                 axis=1)

        hhtable_data.to_sql('hhtable', self._connection, if_exists='append', index=False)
        pd.merge(persons, hhtable_data[['hhnum', 'HouseholdId']], how="left",
                 left_on="HouseholdId", right_on="HouseholdId").to_sql(
            'perstable', self._connection, if_exists="append", index=False)

        print("done")
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

        if maz_controls is None:
            maz_controls = pd.read_csv(f"{self._output_path}/Inputs/{self._config['MazLevelControls']}")

        if taz_controls is None:
            taz_controls = pd.read_csv(f"{self._output_path}/Inputs/{self._config['TazLevelControls']}")

        if meta_controls is None:
            meta_controls = pd.read_csv(f"{self._output_path}/Inputs/{self._config['MetaLevelControls']}")

        metadata = MetaData()

        maz_controls_table = Table('control_totals_maz', metadata,
                                   *[Column(c, _PANDAS_DTYPE_SQL_TYPE[maz_controls.dtypes[c].name]) for c in
                                     maz_controls.columns])
        maz_controls_table.columns[maz_controls.columns[0]].primary_key = True
        maz_controls_table.columns[maz_controls.columns[1]].primary_key = True
        maz_controls_table.columns[maz_controls.columns[2]].primary_key = True
        maz_controls_table.columns[maz_controls.columns[3]].primary_key = True

        taz_controls_table = Table('control_totals_taz', metadata,
                                   *[Column(c, _PANDAS_DTYPE_SQL_TYPE[taz_controls.dtypes[c].name]) for c in
                                     taz_controls.columns])
        taz_controls_table.columns[taz_controls.columns[0]].primary_key = True
        taz_controls_table.columns[taz_controls.columns[1]].primary_key = True
        taz_controls_table.columns[taz_controls.columns[2]].primary_key = True

        meta_controls_table = Table('control_totals_meta', metadata,
                                    *[Column(c, _PANDAS_DTYPE_SQL_TYPE[meta_controls.dtypes[c].name]) for c in
                                      meta_controls.columns])
        meta_controls_table.columns[meta_controls.columns[0]].primary_key = True

        metadata.drop_all(self._engine, [maz_controls_table, taz_controls_table, meta_controls_table])
        metadata.create_all(self._engine)

        maz_controls.to_sql('control_totals_maz', self._connection, if_exists='append', index=False)
        taz_controls.to_sql('control_totals_taz', self._connection, if_exists='replace', index=False)
        meta_controls.to_sql('control_totals_meta', self._connection, if_exists='replace', index=False)
        return

    def __del__(self):
        if self._engine is not None:
            self._engine.dispose()
