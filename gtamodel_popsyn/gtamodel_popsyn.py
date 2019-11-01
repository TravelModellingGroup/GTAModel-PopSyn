import datetime
import os
import subprocess
from logzero import setup_logger
from gtamodel_popsyn._database_processor import DatabaseProcessor
from gtamodel_popsyn.control_totals_builder import ControlTotalsBuilder
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
                 percent_populations: list = None, population_vector_file: str = None):
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
        self._population_vector_file = population_vector_file
        self._columns = []

        if percent_populations is None:
            self._percent_populations = [1.0]
        else:
            self._percent_populations = percent_populations

        #    os.makedirs(f'{config["OutputFolder"]}/{start_time:%Y-%m-%d_%H-%M}/', exist_ok=True)
        # self._output_path = f'{self._config["OutputFolder"]}/{self._start_time:%Y-%m-%d_%H-%M}' if output_path is None else output_path
        # self._logger = setup_logger(name='gtamodel',
        #                       logfile=f'{self._output_path}/gtamodel_popsyn.log')
        # self._logger.info(f'GTAModel PopSyn')
        # self._arguments = arguments

        # iterate over percent_population and perform a run

        for percent_population in self._percent_populations:
            if make_output:
                os.makedirs(
                    f'{config["OutputFolder"]}/{(self._name + "_") if name else ""}{start_time:%Y-%m-%d_%H-%M}_{percent_population}/',
                    exist_ok=True)

            self._arguments = arguments
            self._output_path = f'{self._config["OutputFolder"]}/{(self._name + "_") if name else ""}{self._start_time:%Y-%m-%d_%H-%M}_{percent_population}' if output_path is None else output_path
            self._logger = setup_logger(name='gtamodel', logfile=f'{self._output_path}/gtamodel_popsyn.log')
            self._logger.info(f'GTAModel PopSyn')
            self._summary_report = ValidationReport(self)
            self._control_totals_builder = ControlTotalsBuilder(self, population_vector_file)
            self._input_processor = InputProcessor(self, self._control_totals_builder)
            self._output_processor = OutputProcessor(self, percent_population)
            self._database_processor = DatabaseProcessor(self, percent_population)
            self._settings_processor = SettingsProcessor(self)

        os.makedirs(f'{self._output_path}/Inputs/', exist_ok=True)

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
                         merge_outputs: list = []):
        """
        Generates output files with the synthesized population and other 
        various population vectors required by the
        GTAModel population input.
        :return:
        """
        self._logger.info('Generating population synthesis outputs.')
        self._output_processor.generate_outputs(use_saved, merge_outputs)
        self._logger.info('Output generation has completed processing')

    def initialize_database_with_controls(self, maz,taz,meta):
        self._logger.info('Initializing database.')
        self._database_processor.initialize_database_with_control_files(maz,taz,meta)
        self._logger.info('Database initialization has completed.')
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
        self._run_popsyn3()
        self.generate_outputs(use_saved=False)
        self.generate_summary_report()

    def run(self):
        """
        Runs a complete population synthesis procedure. All input transforms, database processing
        and output generation will be performed.
        :return:
        """
        self.generate_inputs()
        self.initialize_database(
            self._input_processor.processed_persons,
            self._input_processor.processed_households)
        self._run_popsyn3()
        self.generate_outputs(use_saved=False)
        self.generate_summary_report()

    def generate_inputs(self):
        """
        Generates all inputs required for the popsyn3 procedure.
        :return:
        """
        self._logger.info(f'Processing input data.')
        self._input_processor.generate()
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

        popsyn_args = [f'{self._config["Java64Path"]}/bin/java', "-showversion", '-server', '-Xms8000m', '-Xmx15000m',
                       '-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005',
                       f'-XX:ErrorFile={self._output_path}/java_error%p.log',
                       '-cp', ';'.join(classpaths), '-Djppf.config=jppf-clientLocal.properties',
                       f'-Djava.library.path={libpath}',
                       f'-Dlog4j.configuration=file:{self._output_path}/Inputs/log4j.xml',
                       'popGenerator.PopGenerator', f'{self._output_path}/Inputs/settings.xml']

        self._logger.info(popsyn_args)
        p = subprocess.run(popsyn_args, shell=True)

        self._logger.info(p.stdout)
        self._logger.info('PopSyn3 process has completed.')
        return
