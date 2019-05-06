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

    def __init__(self, config, arguments, start_time=datetime.datetime.now()):
        """
        Initializes GTAModelPopSyn class responsible for building control totals and
        processing the input seed data.
        :param config:
        :param arguments:
        :param start_time:
        """
        self._arguments = arguments
        self._logger = setup_logger(name='gtamodel')
        self._config = config
        self._start_time = start_time
        self._output_path = f'{self._config["OutputFolder"]}/{self._start_time:%Y-%m-%d_%H-%M}'
        self._summary_report = ValidationReport(self)
        self._control_totals_builder = ControlTotalsBuilder(self)
        self._input_processor = InputProcessor(self)
        self._output_processor = OutputProcessor(self)
        self._database_processor = DatabaseProcessor(self)
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

    def generate_outputs(self):
        self._logger.info('Generating population synthesis outputs.')
        self._output_processor.generate_outputs()
        self._logger.info('Output generation has completed processing')

    def initialize_database(self, persons=None, households=None):
        """
        Initializes the database and table with required input data for PopSyn3 execution.
        :return:
        """
        self._logger.info('Initializing database.')
        self._database_processor.initialize_database()
        self._logger.info('Database initialization has completed.')
        return

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
        self.generate_outputs()

    def generate_inputs(self):
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
        self._logger.info('PopSyn3 transformed settings file written to output folder.')

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
        subprocess.run([f'{self._config["Java64Path"]}/bin/java', "-showversion", '-server', '-Xms8000m', '-Xmx15000m',
                        '-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005',
                        '-XX:ErrorFile=output/java_error%p.log',
                        '-cp', ';'.join(classpaths), '-Djppf.config=jppf-clientLocal.properties',
                        f'-Djava.library.path={libpath}',
                        'popGenerator.PopGenerator', f'{self._output_path}/Inputs/settings.xml'], shell=True)
        self._logger.info('PopSyn3 process has completed.')
        return
