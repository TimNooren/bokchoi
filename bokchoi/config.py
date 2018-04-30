
import json
import os


class Config:

    def __init__(self, path='.'):

        self.name = None
        self.config_path = os.path.join(path, 'bokchoi_settings.json')

        self.map = {}

        self.loaded = False

    def load(self):

        with open(self.config_path, 'r') as config_file:
            config_json = json.load(config_file)

        self.name = list(config_json.keys())[0]

        self.map = config_json[self.name]
        self.validate(self.map)

        self.loaded = True

    def init(self, name, platform, platform_specific=None):

        default_config = {
            'Platform': platform
            , 'Shutdown': False
            , 'Notebook': True
            , 'App': ''
            , 'Requirements': []
            , platform: platform_specific
        }

        self.map = {name: default_config}

        with open(self.config_path, 'w') as _file:
            json.dump(self.map, _file, indent=4)

    def validate(self, config):
        non_optional = {'App', 'Platform'}

        missing_keys = non_optional - set(config)

        if missing_keys:
            raise AssertionError('Missing keys in config: {}'.format(', '.join(missing_keys)))

    def __getitem__(self, item):
        return self.map[item]
