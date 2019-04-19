import pandas as pd
import json
from gtamodel_popsyn.control_totals_builder import ControlTotalsBuilder
from gtamodel_popsyn.input_processor import SeedDataProcessor


class GTAModelPopSyn():

    def __init__(self,config_file_path='config.json'):
        """
        Initializes GTAModelPopSyn class responsible for building control totals and
        processing the input seed data.
        :param config_file_path: The path to the input configuration.
        """

        self._config = None
        self._seed_data_processor = None
        self._control_totals_builder = None

        try:
            with open(config_file_path) as config_file:
                self.config = json.load(config_file)

        except FileNotFoundError:
            raise FileNotFoundError

        return

    def _init_input_data(self):

        return

    def _init__seed_data_processor(self):
        self._seed_data_processor = SeedDataProcessor(self.__config)

    def _init__control_totals_builder(self):
        self._control_totals_builder = ControlTotalsBuilder(self.__config)
        return

    def build_control_totals(self):

        return