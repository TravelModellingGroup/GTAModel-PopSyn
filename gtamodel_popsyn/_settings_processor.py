from xml.etree import ElementTree

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
        et: ElementTree = xml.etree.ElementTree.parse(self._config["PopSyn3SettingsFile"])
        settings_root = et.getroot()
        settings_root.find('.database/server').text = self._config['DatabaseServer']
        settings_root.find('.database/user').text = self._config['DatabaseUser']
        settings_root.find('.database/password').text = self._config['DatabasePassword']
        settings_root.find('.database/dbName').text = self._config['DatabaseName']
        settings_root.find('.pumsData/outputPersAttributes').text = ', '.join(
            [x.strip(', ') for x in settings_root.find('.pumsData/outputPersAttributes').text.split()])
        et.write(f'{self._output_path}/Inputs/settings.xml')

        self._generate_log4j()
        return

    def _generate_log4j(self):
        """
        Generates the log4j input file. Used to control the output of popsyn3's logger.
        :return:
        """
        xml.etree.ElementTree.register_namespace('', "http://jakarta.apache.org/log4j/")
        xml.etree.ElementTree.register_namespace('log4j', "http://jakarta.apache.org/log4j/")
        et = xml.etree.ElementTree.parse('runtime/config/log4j.xml')
        settings_root = et.getroot()
        settings_root.find(".//*[@name='FILE']/param[1]").set('value', f'{self._output_path}/popsyn3_log.log')

        with open(f'{self._output_path}/Inputs/log4j.xml', "w", encoding='UTF-8') as xf:
            doc_type = '<!DOCTYPE log4j:configuration SYSTEM "log4j.dtd">'
            tostring = xml.etree.ElementTree.tostring(settings_root).decode('utf-8')
            file = f"{doc_type}{tostring}"
            xf.write(file)
