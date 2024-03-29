import datetime
import os
import subprocess
from io import TextIOWrapper

from logzero import setup_logger
from gtamodel_popsyn._database_processor import DatabaseProcessor
from gtamodel_popsyn.control_totals_builder import ControlTotalsBuilder
from gtamodel_popsyn.gtamodel_popsyn_config import GTAModelPopSynConfig
from gtamodel_popsyn.input_processor import InputProcessor
from gtamodel_popsyn.output_processor import OutputProcessor
from gtamodel_popsyn.validation_report import ValidationReport
from gtamodel_popsyn._settings_processor import SettingsProcessor


class GTAModelPopSyn(object):
    """
    Main driver class of the gtamodel_popsyn module. An instance of this class is responsible for running
    and automating all processing steps of the synthesis procedure.
    """

    @property
    def output_path(self):
        return self._output_path

    @property
    def config(self):
        return self._config

    @property
    def popsyn_config(self):
        return self._popsyn_config

    @property
    def arguments(self):
        return self._arguments

    @property
    def logger(self):
        return self._logger

    @property
    def columns(self):
        return self._columns

    def __init__(self, config, arguments, start_time=datetime.datetime.now(), name=None, output_path=None,
                 make_output=True,
                 percent_populations: list = None, population_vector: TextIOWrapper = None):
        """
        Initializes GTAModelPopSyn class responsible for building control totals and
        processing the input seed data.
        :param config:
        :param arguments:
        :param start_time:
        """
        self._config = config
        self._start_time = start_time
        self._name = name
        self._population_vector = population_vector
        self._columns = []

        if percent_populations is None:
            self._percent_populations = [1.0]
        else:
            self._percent_populations = percent_populations
        for percent_population in self._percent_populations:
            if make_output:
                os.makedirs(
                    f'{config["OutputFolder"]}/{(self._name + "_") if name else ""}'
                    f'{start_time:%Y-%m-%d_%H-%M}_{percent_population}/',
                    exist_ok=True)

            self._arguments = arguments
            self._output_path = f'{self._config["OutputFolder"]}/{(self._name + "_") if name else ""}' \
                                f'{self._start_time:%Y-%m-%d_%H-%M}_' \
                                f'{percent_population}' if output_path is None else output_path
            self._logger = setup_logger(name='gtamodel', logfile=f'{self._output_path}/gtamodel_popsyn.log')
            self._logger.info(f'GTAModel PopSyn')
            self._popsyn_config = GTAModelPopSynConfig(self)
            self._popsyn_config.initialize()
            self._summary_report = ValidationReport(self)
            self._summary_report.popsyn_config = self._popsyn_config
            self._control_totals_builder = ControlTotalsBuilder(self)
            self._control_totals_builder.popsyn_config = self._popsyn_config
            self._input_processor = InputProcessor(self, self._control_totals_builder)
            self._input_processor.popsyn_config = self._popsyn_config
            self._output_processor = OutputProcessor(self, percent_population)
            self._output_processor.popsyn_config = self._popsyn_config
            self._database_processor = DatabaseProcessor(self, percent_population)
            self._settings_processor = SettingsProcessor(self)

        os.makedirs(f'{self._output_path}/Inputs/', exist_ok=True)
        self._popsyn_config = GTAModelPopSynConfig(self)
        self._copy_config_files()
        return

    def generate_summary_report(self):
        """
        Runs the summary report tool that generates validation data for the
        the synthesized population.
        This method require an output to have already been generated.
        :return:
        """
        self._logger.info('Generating summary report.')
        self._summary_report.generate()
        self._logger.info('Summary report has been generated.')

    def generate_outputs(self, use_saved: bool = False,
                         merge_outputs=None):
        """
        Generates output files with the synthesized population and other 
        various population vectors required by the
        GTAModel population input.
        :return:
        """
        if merge_outputs is None:
            merge_outputs = []
        self._logger.info('Generating population synthesis outputs.')
        self._output_processor.generate_outputs(use_saved, merge_outputs)
        self._logger.info('Output generation has completed processing')

    def _copy_config_files(self):
        import json
        with open(self._output_path + '/config.json', 'w') as outfile:
            json.dump(self._config, outfile)

    def initialize_database_with_controls(self, maz_file: str, taz_file: str, meta_file: str, gen_puma=False):
        """
        Initializes the database using control totals passed from an already existing set of files.
        If a population vector is also used in the procedure, it will be applied before the controls
        are moved to to the database.
        @param maz_file:
        @param taz_file:
        @param meta_file:
        @param gen_puma:
        @return:
        """
        import pandas as pd
        self._logger.info('Initializing database with file controls.')
        if self._population_vector is not None:
            # apply forecast / population vector changes in place
            self._logger.info('Applying population vector to control totals.')
            (maz, taz, meta) = self._control_totals_builder.apply_population_vector(
                maz_file, taz_file, meta_file, self._population_vector)
            self._database_processor.initialize_database_with_existing_controls(maz, taz, meta)
        else:
            self._database_processor.initialize_database_with_control_files(maz_file, taz_file, meta_file, gen_puma)
        self._logger.info('Database and control initialization has completed.')
        return

    def initialize_database(self, persons=None, households=None):
        """
        Initializes the database and table with required input data for PopSyn3 execution.
        :param persons:
        :param households:
        :return:
        """
        self._logger.info('Initializing database.')
        self._database_processor.initialize_database()
        self._logger.info('Database initialization has completed.')
        return

    def post_input_run(self):
        # self.initialize_database(
        #   self._input_processor.processed_persons,
        #    self._input_processor.processed_households)
        # copy input files to output directory

        self._run_popsyn3()
        self.generate_outputs(use_saved=False)
        self.generate_summary_report()

    def run(self):
        """
        Runs a complete population synthesis procedure. All input transforms, database processing
        and output generation will be performed.
        :return:
        """
        self.generate_inputs(True)
        self.initialize_database(
            self._input_processor.processed_persons,
            self._input_processor.processed_households)
        self._run_popsyn3()
        self.generate_outputs(use_saved=False)
        self.generate_summary_report()

    def generate_inputs(self, build_controls: bool = True):
        """
        Generates all inputs required for the popsyn3 procedure.
        :return:
        """
        self._logger.info(f'Processing input data.')
        self._input_processor.generate(build_controls)
        self._logger.info(f'Input data has completed processing.')
        return

    def _generate_popsyn3_settings(self):
        """
        Cleans and generates the popsyn3 XML settings file.
        :return:
        """
        self._settings_processor.generate_settings()
        self._logger.info(
            'PopSyn3 transformed settings file written to output folder.')

    def _run_popsyn3(self):
        """
        Starts the execution of the popsyn3 subprocess. PopSyn3's input settings are transformed
        and cleaned as part of this step.
        :return:
        """
        self._generate_popsyn3_settings()
        # configure java classpaths
        classpath_root = 'runtime/config'
        classpaths = [f'{classpath_root}', 'runtime/*', 'runtime/lib/*',
                      'runtime/lib/JPFF-3.2.2/JPPF-3.2.2-admin-ui/lib/*']
        libpath = 'runtime/lib'

        # run popsyn3 subprocess
        self._logger.info('PopSyn3 execution started.')

        popsyn_args = [f'{self._config["Java64Path"]}/bin/java', "-showversion", '-server', '-Xms14000m', '-Xmx24000m',
                       '-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005',
                       f'-XX:ErrorFile={self._output_path}/java_error%p.log',
                       '-cp', ';'.join(classpaths), '-Djppf.config=jppf-clientLocal.properties',
                       f'-Djava.library.path={libpath}',
                       f'-Dlog4j.configuration=file:{self._output_path}/Inputs/log4j.xml',
                       'popGenerator.PopGenerator', f'{self._output_path}/Inputs/settings.xml']

        self._logger.info(popsyn_args)
        p = subprocess.run(popsyn_args, shell=True)
        if p.returncode != 0:
            self._logger.error("Error occurred in PopSyn3")

        self._logger.info(p.stdout)
        self._logger.error(p.stderr)
        self._logger.info('PopSyn3 process has completed.')
        return
