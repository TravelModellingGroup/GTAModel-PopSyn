import pandas as pd
import json
from gtamodel_popsyn._database_processor import DatabaseProcessor
from gtamodel_popsyn.control_totals_builder import ControlTotalsBuilder
from gtamodel_popsyn.input_processor import InputProcessor
from gtamodel_popsyn.output_processor import OutputProcessor
from gtamodel_popsyn.summary_report import SummaryReport
from logzero import logger, setup_logger


class GTAModelPopSyn(object):

    def __init__(self, config):
        """
        Initializes GTAModelPopSyn class responsible for building control totals and
        processing the input seed data.
        :param config_file_path: The path to the input configuration.
        """
        self._logger = setup_logger(name='gtamodel')
        self._config = config
        self._summary_report = SummaryReport(config)
        self._control_totals_builder = ControlTotalsBuilder(self._config)
        self._input_processor = InputProcessor(self._config)
        self._output_processor = OutputProcessor(self._config)
        self._database_processor = DatabaseProcessor(self._config)

        return

    def generate_summary_report(self):
        """
        Runs the summary report tool that generates validation data for the
        the synthesized population.
        This method require an output to have already been generated.
        :return:
        """
        logger.info('Generating summary report.')
        self._summary_report.generate()
        logger.info('Summary report has been generated.')

    def generate_outputs(self):
        self._logger.info('Generating population synthesis outputs.')
        self._output_processor.generate_outputs()
        self._logger.info('Output generation has completed processing')

    def initialize_database(self, persons=None, households=None):
        """
        Initializes the database and table with required input data for PopSyn3 execution.
        :return:
        """
        self._database_processor.initialize_database()
        return

    def run(self):
        """

        :return:
        """
        self.generate_inputs()
        self.initialize_database(
            self._input_processor.processed_persons,
            self._input_processor.processed_households)

        self._run_popsyn3()

    def generate_inputs(self):
        self._logger.info(f'Processing input data.')
        self._input_processor.generate()
        self._logger.info(f'Input data has completed processing.')
        return

    def _run_popsyn3(self):
        return
