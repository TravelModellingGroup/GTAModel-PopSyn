from logzero import setup_logger


class GTAModelPopSynProcessor(object):
    """

    """

    def __init__(self, gtamodel_popsyn_instance):
        """
        :param gtamodel_popsyn_instance:
        :return:
        """
        self._logger = gtamodel_popsyn_instance.logger
        self._gtamodel_popsyn_instance = gtamodel_popsyn_instance
        self._output_folder = gtamodel_popsyn_instance.output_path
        self._output_path = gtamodel_popsyn_instance.output_path
        self._config = gtamodel_popsyn_instance.config
        self._arguments = gtamodel_popsyn_instance.arguments
