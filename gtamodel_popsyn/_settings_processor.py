from gtamodel_popsyn._gtamodel_popsyn_processor import GTAModelPopSynProcessor
import xml.etree.ElementTree


class SettingsProcessor(GTAModelPopSynProcessor):
    """
    Generates a clean and validated settings.xml that the PopSyn3 software requires
    for configuration.
    """

    def __init__(self, gtamodel_popsyn_instance):
        """
        Initialization
        :param gtamodel_popsyn_instance:
        """
        GTAModelPopSynProcessor.__init__(self, gtamodel_popsyn_instance)

    def generate_settings(self):
        """
        Generate the settings XML and write it to file.
        :return:
        """
        et = xml.etree.ElementTree.parse(self._config["PopSyn3SettingsFile"])
        settings_root = et.getroot()
        settings_root.find('.database/server').text = self._config['DatabaseServer']
        settings_root.find('.database/user').text = self._config['DatabaseUser']
        settings_root.find('.database/password').text = self._config['DatabasePassword']
        settings_root.find('.database/dbName').text = self._config['DatabaseName']
        settings_root.find('.pumsData/outputPersAttributes').text = ', '.join(
            [x.strip(', ') for x in settings_root.find('.pumsData/outputPersAttributes').text.split()])
        et.write(f'{self._output_path}/Inputs/settings.xml')
        return
