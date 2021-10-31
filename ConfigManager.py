from configparser import ConfigParser


class ConfigManager:

    def __init__(self, file):
        self.config = ConfigParser()
        self.config.read(file)

    def get_db(self, param):
        return self.config.get("DB", param)
