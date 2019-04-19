
class SeedDataProcessor():
    """
    Processes input seed data (households and persons). Applies transformations to invalid persons or households
    and filters records.
    """

    def __init__(self, config):

        self._config = config
        return