from shutil import copyfile

from pandas import DataFrame
import pandas as pd
from sqlalchemy import Table, Column, Integer, MetaData, FLOAT, VARCHAR, PrimaryKeyConstraint
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from gtamodel_popsyn._gtamodel_popsyn_processor import GTAModelPopSynProcessor


class DatabaseProcessor(GTAModelPopSynProcessor):
    """
    Processes input database tables for PopSyn3 - will create them where they do not exist,
    and read in the appropriate input data.
    """

    PANDAS_DTYPE_SQL_TYPE = {
        'int64': Integer,
        'float64': FLOAT,
        'float32': FLOAT,
        'object': VARCHAR(1)
    }

    def __init__(self, gtamodel_popsyn_instance, percent_population: float):
        """

        :param gtamodel_popsyn_instance:
        :param percent_population:
        """
        GTAModelPopSynProcessor.__init__(self, gtamodel_popsyn_instance)
        self._engine: Engine = None
        self._connection = None
        self._percent_population = percent_population

    def _init_connection(self):
        self._engine = create_engine(
            f'mysql+pymysql://{self._config["DatabaseUser"]}:{self._config["DatabasePassword"]}'
            f'@{self._config["DatabaseServer"]}/{self._config["DatabaseName"]}')
        self._connection = self._engine.connect()

    def initialize_database(self, persons=None, households=None):
        """
        Initializes all databases and tables for PopSyn3 execution
        :param persons: Processed persons dataframe.
        :param households: Processed households dataframe.
        :return:
        """

        self._init_connection()
        self._initialize_record_tables(persons, households)
        self._initialize_control_tables()
        self._connection.close()

    def initialize_database_with_control_files(self, maz: str, taz: str, meta: str, gen_puma=False):
        """

        @param maz:
        @param taz:
        @param meta:
        """
        self._init_connection()
        self.initialize_control_tables_from_file(maz, taz, meta, gen_puma)
        self._initialize_record_tables(None, None)
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
            persons: pd.DataFrame = pd.read_csv(
                f"{self._config['ProcessedPersonsSeedFile']}")

        if households is None:
            households: pd.DataFrame = pd.read_csv(
                f"{self._config['ProcessedHouseholdsSeedFile']}")

        metadata = MetaData()

        households_table_columns = [Column(c, self.PANDAS_DTYPE_SQL_TYPE[households.dtypes[c].name]) for c in
                                    households.columns]
        households_table = Table('pumf_hh', metadata, *households_table_columns)
        for c in households.columns:
            if households.dtypes[c].name == 'float64':
                households[c] = households[c].astype('float32')
        households_table.columns[households.columns[0]].primary_key = True

        persons_table_columns = [Column(c, self.PANDAS_DTYPE_SQL_TYPE[persons.dtypes[c].name]) for c in persons.columns]
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
        hhtable = Table('hhtable', metadata, *[Column(c, self.PANDAS_DTYPE_SQL_TYPE[households.dtypes[c].name]) for c in
                                               households.columns],
                        Column('hhnum', Integer, unique=True, autoincrement=True, nullable=False))

        perstable = Table('perstable', metadata,
                          *[Column(c, self.PANDAS_DTYPE_SQL_TYPE[persons.dtypes[c].name]) for c in
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

        return

    def initialize_control_tables_from_file(self, maz_controls_file, taz_controls_file, meta_controls_file,
                                            gen_puma=False):
        """
        Load control files into database based on passed files.
        @param maz_controls_file:
        @param taz_controls_file:
        @param meta_controls_file:
        """
        self._logger.info("Copying control files to output directory")
        copyfile(maz_controls_file, f"{self._output_path}/Inputs/{self._config['MazLevelControls']}")
        copyfile(taz_controls_file, f"{self._output_path}/Inputs/{self._config['TazLevelControls']}")
        copyfile(meta_controls_file, f"{self._output_path}/Inputs/{self._config['MetaLevelControls']}")
        maz_controls = pd.read_csv(f"{maz_controls_file}")
        taz_controls = pd.read_csv(f"{taz_controls_file}")
        meta_controls = pd.read_csv(f"{meta_controls_file}")
        self._initialize_control_tables(maz_controls, taz_controls, meta_controls)

    def _generate_puma_values(self, population: list):
        """
        Generates a mapping of puma geography for taz/maz roughly equally dividided
        by the number configured in the configuration.
        @param population:
        @return:
        """
        if 'GeneratePumas ' in self._config:
            # puma_count = self._config['GeneratePumas']
            pd_pop = population[-1] / int(self._config['GeneratePumas'])
            pd_list = []
            curr_pd = 1
            curr_sum = population[0]
            for t in population:
                pd_list.append(curr_pd)
                if (t - curr_sum) > pd_pop:
                    curr_pd = curr_pd + 1
                    curr_sum = t
            return pd_list
        else:
            return []

    def _initialize_control_tables(self, maz_controls: pd.DataFrame = None,
                                   taz_controls: pd.DataFrame = None,
                                   meta_controls: pd.DataFrame = None,
                                   gen_puma=False):
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

        if gen_puma:
            pd_list = self._generate_puma_values(maz_controls.puma.cumsum().tolist())
            if len(pd_list) == len(maz_controls):
                maz_controls['puma'] = pd_list
                taz_controls['puma'] = pd_list

        maz_columns = [
            Column(maz_controls.columns[0], self.PANDAS_DTYPE_SQL_TYPE[maz_controls.dtypes[0].name], primary_key=True),
            Column(maz_controls.columns[1], self.PANDAS_DTYPE_SQL_TYPE[maz_controls.dtypes[1].name], primary_key=True),
            Column(maz_controls.columns[2], self.PANDAS_DTYPE_SQL_TYPE[maz_controls.dtypes[2].name], primary_key=True),
            Column(maz_controls.columns[3], self.PANDAS_DTYPE_SQL_TYPE[maz_controls.dtypes[3].name], primary_key=True)]
        maz_columns.extend([Column(c, self.PANDAS_DTYPE_SQL_TYPE[maz_controls.dtypes[c].name]) for c in
                            maz_controls.columns[4:]])
        maz_controls_table = Table('control_totals_maz', metadata,
                                   *maz_columns, PrimaryKeyConstraint('region', 'puma', 'taz', 'maz',
                                                                      name='maz_pk'))

        taz_columns = [Column(c, self.PANDAS_DTYPE_SQL_TYPE[taz_controls.dtypes[c].name]) for c in
                       taz_controls.columns]
        taz_columns[0].primary_key = True
        taz_columns[1].primary_key = True
        taz_columns[2].primary_key = True
        taz_controls_table = Table('control_totals_taz', metadata,
                                   *taz_columns)

        meta_columns = [Column(c, self.PANDAS_DTYPE_SQL_TYPE[meta_controls.dtypes[c].name]) for c in
                        meta_controls.columns]
        meta_columns[0].primary_key = True
        meta_controls_table = Table('control_totals_meta', metadata, *meta_columns)

        metadata.drop_all(self._engine, [maz_controls_table, taz_controls_table, meta_controls_table])
        metadata.create_all(self._engine)

        maz_controls.to_sql('control_totals_maz', self._connection, if_exists='replace', index=False)
        taz_controls.to_sql('control_totals_taz', self._connection, if_exists='replace', index=False)
        meta_controls.to_sql('control_totals_meta', self._connection, if_exists='replace', index=False)

        self._connection.execute('ALTER TABLE `control_totals_maz` ADD PRIMARY KEY (`region`,`puma`,`taz`,`maz`);')
        self._connection.execute('ALTER TABLE `control_totals_taz` ADD PRIMARY KEY (`region`,`puma`,`taz`);')
        self._connection.execute('ALTER TABLE `control_totals_meta` ADD PRIMARY KEY (`region`);')
        return

    def __del__(self):
        return
