import pandas as pd
import json


class ControlTotalsBuilder(object):
    """
    ControlTotalsBuilder analyzes the input household and persons data and generates control totals
    for the specified attributes across all levels of geography.
    """
    def __init__(self, config):
        """
        Initializes the control totals builder.
        :param config_file:
        """

        self._config = config

    def build(self):
        """
        Builds the control tables (represented as dataframes).
        :return:
        """

        return

    def _process_persons(self):
        return

    def _process_households(self):
        return

    def write_control_files(self):
        return
