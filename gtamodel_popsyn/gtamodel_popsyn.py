import pandas as pd
import json
from gtamodel_popsyn.control_totals_builder import ControlTotalsBuilder
from gtamodel_popsyn.input_processor import InputProcessor
from gtamodel_popsyn.output_processor import OutputProcessor
from gtamodel_popsyn.summary_report import SummaryReport
from logzero import logger, setup_logger
import logzero

class GTAModelPopSyn(object):

    def __init__(self, config):
        """
        Initializes GTAModelPopSyn class responsible for building control totals and
        processing the input seed data.
        :param config_file_path: The path to the input configuration.
        """
        self._config = config
        self._summary_report = SummaryReport(config)
        self._control_totals_builder = ControlTotalsBuilder(self._config)
        self._input_processor = InputProcessor(self._config)
        self._output_processor = OutputProcessor(self._config)
        self._logger = setup_logger(name='gtamodel')
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

    def run(self):
        self.generate_inputs()

        """


        :return:
        """
    def generate_inputs(self):
        self._logger.info(f'Processing input data.')
        self._input_processor.generate()
        self._logger.info(f'Input data has completed processing.')
        return